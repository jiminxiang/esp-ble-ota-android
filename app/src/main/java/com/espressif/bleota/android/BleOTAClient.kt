package com.espressif.bleota.android

import android.annotation.SuppressLint
import android.bluetooth.BluetoothDevice
import android.bluetooth.BluetoothGatt
import android.bluetooth.BluetoothGattCharacteristic
import android.bluetooth.BluetoothGattDescriptor
import android.bluetooth.BluetoothGattService
import android.content.Context
import android.os.Handler
import android.os.Looper
import android.util.Log
import com.espressif.bleota.android.message.BleOTAMessage
import com.espressif.bleota.android.message.EndCommandAckMessage
import com.espressif.bleota.android.message.StartCommandAckMessage
import no.nordicsemi.android.ble.observer.ConnectionObserver
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.Closeable
import java.util.LinkedList
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread

@SuppressLint("MissingPermission")
class BleOTAClient(
    private val context: Context,
    private val device: BluetoothDevice,
    private val bin: ByteArray,
) : Closeable {

    /** Whole image size in bytes (for UI throughput stats). */
    val firmwareSizeBytes: Int get() = bin.size

    private val manager = EspBleOtaBleManager(context, this)

    companion object {
        internal const val TAG = "BleOTAClient"
        internal const val DEBUG = false

        private const val COMMAND_ID_START = 0x0001
        private const val COMMAND_ID_END = 0x0002
        private const val COMMAND_ID_ACK = 0x0003

        const val COMMAND_ACK_ACCEPT = 0x0000
        const val COMMAND_ACK_REFUSE = 0x0001

        private const val BIN_ACK_SUCCESS = 0x0000
        private const val BIN_ACK_CRC_ERROR = 0x0001
        private const val BIN_ACK_SECTOR_INDEX_ERROR = 0x0002
        private const val BIN_ACK_PAYLOAD_LENGTH_ERROR = 0x0003

        internal const val MTU_SIZE = 500
        private const val EXPECT_PACKET_SIZE = 463

        private const val SECTOR_ACK_TIMEOUT_MS = 30_000L

        /** Max consecutive INDEX_ERR ACKs handled by resync before aborting OTA and disconnecting. */
        private const val MAX_CONSECUTIVE_INDEX_ERR_RESYNC = 10

        internal val SERVICE_UUID = bleUUID("8018")
        internal val CHAR_RECV_FW_UUID = bleUUID("8020")
        internal val CHAR_PROGRESS_UUID = bleUUID("8021")
        internal val CHAR_COMMAND_UUID = bleUUID("8022")
        internal val CHAR_CUSTOMER_UUID = bleUUID("8023")

        /** Pipeline depth when START ACK byte 6 is zero (legacy peripheral without window field). */
        private const val LEGACY_DEFAULT_SECTOR_SEND_WINDOW = 2

        private const val MIN_SECTOR_SEND_WINDOW = 1
        private const val MAX_SECTOR_SEND_WINDOW = 64

        internal fun bleDebugLog(): Boolean = DEBUG
    }

    var packetSize = 20

    var service: BluetoothGattService? = null
    var recvFwChar: BluetoothGattCharacteristic? = null
    var progressChar: BluetoothGattCharacteristic? = null
    var commandChar: BluetoothGattCharacteristic? = null
    var customerChar: BluetoothGattCharacteristic? = null

    private var callback: GattCallback? = null
    private val packets = LinkedList<ByteArray>()
    private val sectorAckIndex = AtomicInteger(0)
    /** Counts consecutive sector INDEX_ERR ACKs without an intervening success ACK. */
    private val consecutiveIndexErrResyncCount = AtomicInteger(0)
    private val sectorAckMark = ByteArray(0)

    /** Sectors fully written (past [sectorAckMark]) but not yet released by BIN_ACK_SUCCESS. */
    private var sectorsInFlight: Int = 0

    /** From START ACK byte 6; fallback [LEGACY_DEFAULT_SECTOR_SEND_WINDOW] if zero. */
    private var sectorSendWindow: Int = LEGACY_DEFAULT_SECTOR_SEND_WINDOW

    private val mainHandler = Handler(Looper.getMainLooper())
    private var binTransferActive = false

    private val sectorAckTimeoutRunnable = Runnable {
        Log.w(TAG, "sector ACK timeout")
        if (binTransferActive) {
            failOtaTransfer()
            callback?.onError(BleOTAErrors.SECTOR_ACK_TIMEOUT)
        }
    }

    fun connect(callback: GattCallback) {
        Log.i(TAG, "start OTA")
        stop()

        this.callback = callback
        callback.client = this
        manager.connectWithObserver(device, callback)
    }

    fun stop() {
        cancelOtaTimeouts()
        binTransferActive = false
        manager.shutdown()
        callback = null

        sectorsInFlight = 0
        packets.clear()
        consecutiveIndexErrResyncCount.set(0)
        sectorSendWindow = LEGACY_DEFAULT_SECTOR_SEND_WINDOW
        service = null
        recvFwChar = null
        progressChar = null
        commandChar = null
        customerChar = null
    }

    override fun close() {
        stop()
    }

    fun ota() {
        manager.enqueueEnableNotificationsThen { postCommandStart() }
    }

    internal fun onGattServicesResolved(
        svc: BluetoothGattService?,
        recv: BluetoothGattCharacteristic?,
        progress: BluetoothGattCharacteristic?,
        command: BluetoothGattCharacteristic?,
        customer: BluetoothGattCharacteristic?,
    ) {
        service = svc
        recvFwChar = recv
        progressChar = progress
        commandChar = command
        customerChar = customer
    }

    internal fun onGattServicesInvalidated() {
        service = null
        recvFwChar = null
        progressChar = null
        commandChar = null
        customerChar = null
    }

    internal fun applyNegotiatedMtu(mtu: Int) {
        packetSize = if (mtu > 200) EXPECT_PACKET_SIZE else 20
    }

    internal fun notifyMtuNegotiated(mtu: Int, success: Boolean) {
        callback?.onMtuNegotiated(mtu, success)
    }

    private fun cancelOtaTimeouts() {
        mainHandler.removeCallbacks(sectorAckTimeoutRunnable)
    }

    private fun scheduleSectorAckTimeout() {
        mainHandler.removeCallbacks(sectorAckTimeoutRunnable)
        mainHandler.postDelayed(sectorAckTimeoutRunnable, SECTOR_ACK_TIMEOUT_MS)
    }

    private fun failOtaTransfer() {
        binTransferActive = false
        packets.clear()
        consecutiveIndexErrResyncCount.set(0)
    }

    internal fun onRecvFwWriteFailed(gattStatus: Int) {
        cancelOtaTimeouts()
        failOtaTransfer()
        Log.w(TAG, "recv FW write failed: gattStatus=$gattStatus")
        callback?.onError(BleOTAErrors.CHARACTERISTIC_WRITE_FAILED)
    }

    internal fun onDisconnectedDuringTransfer() {
        cancelOtaTimeouts()
        val wasActive = binTransferActive
        failOtaTransfer()
        sectorAckIndex.set(0)
        if (wasActive) {
            callback?.onError(BleOTAErrors.DISCONNECTED_DURING_OTA)
        }
    }

    /**
     * Rebuilds the FW write queue starting at [firstSector] (0-based), and sets
     * [sectorAckIndex] to the same value so the next sector ACK matches device expectation.
     */
    private fun rebuildPacketsFromSector(firstSector: Int) {
        val sectors = ArrayList<ByteArray>()
        ByteArrayInputStream(bin).use {
            val buf = ByteArray(4096)
            while (true) {
                val read = it.read(buf)
                if (read == -1) {
                    break
                }
                sectors.add(buf.copyOf(read))
            }
        }
        if (sectors.isEmpty()) {
            sectorAckIndex.set(0)
            packets.clear()
            return
        }

        val from = firstSector.coerceIn(0, sectors.lastIndex)
        sectorAckIndex.set(from)
        packets.clear()

        val block = ByteArray(packetSize - 3)
        for (element in sectors.withIndex()) {
            if (element.index < from) {
                continue
            }
            val sector = element.value
            val index = element.index
            val stream = ByteArrayInputStream(sector)
            var sequence = 0
            while (true) {
                val read = stream.read(block)
                if (read == -1) {
                    break
                }
                var crc = 0
                val bLast = stream.available() == 0
                if (bLast) {
                    sequence = -1
                    crc = EspCRC16.crc(sector)
                }

                val len = if (bLast) read + 5 else read + 3
                val packet = ByteArrayOutputStream(len).use {
                    it.write(index and 0xff)
                    it.write(index shr 8 and 0xff)
                    it.write(sequence)
                    it.write(block, 0, read)
                    if (bLast) {
                        it.write(crc and 0xff)
                        it.write(crc shr 8 and 0xff)
                    }
                    it.toByteArray()
                }

                ++sequence

                packets.add(packet)
            }
            packets.add(sectorAckMark)
        }
        if (DEBUG) {
            Log.d(TAG, "rebuildPacketsFromSector: from=$from sectors=${sectors.size} queue=${packets.size}")
        }
    }

    private fun initPackets() {
        cancelOtaTimeouts()
        sectorsInFlight = 0
        consecutiveIndexErrResyncCount.set(0)
        rebuildPacketsFromSector(0)
    }

    private fun genCommandPacket(id: Int, payload: ByteArray): ByteArray {
        val packet = ByteArray(20)
        packet[0] = (id and 0xff).toByte()
        packet[1] = (id shr 8 and 0xff).toByte()
        System.arraycopy(payload, 0, packet, 2, payload.size)
        val crc = EspCRC16.crc(packet, 0, 18)
        packet[18] = (crc and 0xff).toByte()
        packet[19] = (crc shr 8 and 0xff).toByte()
        return packet
    }

    private fun postCommandStart() {
        Log.i(TAG, "postCommandStart")
        val binSize = bin.size
        val payload = byteArrayOf(
            (binSize and 0xff).toByte(),
            (binSize shr 8 and 0xff).toByte(),
            (binSize shr 16 and 0xff).toByte(),
            (binSize shr 24 and 0xff).toByte(),
        )
        val packet = genCommandPacket(COMMAND_ID_START, payload)
        manager.enqueueCommandWrite(packet)
    }

    private fun receiveCommandStartAck(status: Int, sendWindowByte: Int) {
        when (status) {
            COMMAND_ACK_ACCEPT -> {
                sectorSendWindow = when {
                    sendWindowByte > 0 ->
                        sendWindowByte.coerceIn(MIN_SECTOR_SEND_WINDOW, MAX_SECTOR_SEND_WINDOW)
                    else -> LEGACY_DEFAULT_SECTOR_SEND_WINDOW
                }
                Log.i(TAG, "receiveCommandStartAck: status=$status sectorSendWindow=$sectorSendWindow")
                postBinData()
            }
            COMMAND_ACK_REFUSE -> {
                Log.i(TAG, "receiveCommandStartAck: status=$status")
                callback?.onError(BleOTAErrors.START_REFUSED)
            }
        }

        val message = StartCommandAckMessage(status)
        callback?.onOTA(message)
    }

    private fun postCommandEnd() {
        Log.i(TAG, "postCommandEnd")
        val payload = ByteArray(0)
        val packet = genCommandPacket(COMMAND_ID_END, payload)
        manager.enqueueCommandWrite(packet)
    }

    private fun receiveCommandEndAck(status: Int) {
        Log.i(TAG, "receiveCommandEndAck: status=$status")
        binTransferActive = false
        when (status) {
            COMMAND_ACK_ACCEPT -> {
            }
            COMMAND_ACK_REFUSE -> {
                callback?.onError(BleOTAErrors.END_REFUSED)
            }
        }

        val message = EndCommandAckMessage(status)
        callback?.onOTA(message)
    }

    private fun postBinData() {
        thread {
            binTransferActive = true
            initPackets()
            postNextPacket()
        }
    }

    private fun postNextPacket() {
        val packet = packets.pollFirst()
        if (packet == null) {
            if (sectorsInFlight == 0) {
                cancelOtaTimeouts()
                postCommandEnd()
            } else {
                scheduleSectorAckTimeout()
            }
            return
        }
        if (packet === sectorAckMark) {
            sectorsInFlight++
            if (DEBUG) {
                Log.d(TAG, "postNextPacket: sector sent, sectorsInFlight=$sectorsInFlight")
            }
            val hasMore = packets.isNotEmpty()
            if (hasMore) {
                if (DEBUG) {
                    Log.d(TAG, "postNextPacket: has more packets, sectorsInFlight=$sectorsInFlight")
                }
                if (sectorsInFlight < sectorSendWindow) {
                    postNextPacket()
                } else {
                    Log.d(TAG, "postNextPacket: sectors in flight >= $sectorSendWindow, wait for ACK")
                }
            }
            return
        }
        if (DEBUG) {
            val len = minOf(packet.size, 40)
            val hexString = packet.take(len).joinToString(" ") { "%02X".format(it) }
            Log.d(TAG, "postNextPacket: enqueue FW write: [$hexString] (len=${packet.size})")
        }

        manager.enqueueFirmwarePacket(
            data = packet,
            onComplete = { postNextPacket() },
            onFailRequeue = { packets.offerFirst(packet) },
        )
    }

    internal fun parseSectorAck(data: ByteArray) {
        cancelOtaTimeouts()
        try {
            if (data.size < 20) {
                Log.w(TAG, "parseSectorAck: short payload, size=${data.size}")
                callback?.onError(BleOTAErrors.SECTOR_ACK_TOO_SHORT)
                failOtaTransfer()
                return
            }
            val crcRecv = u16le(data, 18)
            val crcCalc = EspCRC16.crc(data, 0, 18)
            if (crcRecv != crcCalc) {
                Log.w(TAG, "parseSectorAck: CRC mismatch recv=0x${crcRecv.toString(16)} calc=0x${crcCalc.toString(16)}")
                callback?.onError(BleOTAErrors.SECTOR_ACK_CRC_INVALID)
                failOtaTransfer()
                return
            }

            val expectIndex = sectorAckIndex.get()
            val ackIndex = u16le(data, 0)
            if (ackIndex != expectIndex) {
                Log.w(TAG, "parseSectorAck: index $ackIndex, expect $expectIndex")
                callback?.onError(BleOTAErrors.SECTOR_ACK_INDEX_MISMATCH)
                failOtaTransfer()
                return
            }

            val ackStatus = u16le(data, 2)
            val nextExpected = u16le(data, 4)
            Log.d(TAG, "parseSectorAck: index=$ackIndex, status=$ackStatus, nextExpected=$nextExpected")

            when (ackStatus) {
                BIN_ACK_SUCCESS -> {
                    consecutiveIndexErrResyncCount.set(0)
                    val expectedNext = (ackIndex + 1) and 0xffff
                    if (nextExpected != expectedNext) {
                        Log.w(TAG, "parseSectorAck: nextExpected mismatch got=$nextExpected want=$expectedNext")
                        callback?.onError(BleOTAErrors.SECTOR_ACK_NEXT_MISMATCH)
                        failOtaTransfer()
                        return
                    }
                    sectorAckIndex.incrementAndGet()
                    sectorsInFlight--
                    if (sectorsInFlight < 0) {
                        Log.w(TAG, "parseSectorAck: sectorsInFlight underflow, clamping to 0")
                        sectorsInFlight = 0
                    }
                    postNextPacket()
                }
                BIN_ACK_CRC_ERROR -> {
                    callback?.onError(BleOTAErrors.BIN_CRC_REJECT)
                    failOtaTransfer()
                }
                BIN_ACK_SECTOR_INDEX_ERROR -> {
                    val devExpectIndex = u16le(data, 4)
                    val attempt = consecutiveIndexErrResyncCount.incrementAndGet()
                    if (attempt >= MAX_CONSECUTIVE_INDEX_ERR_RESYNC) {
                        Log.e(
                            TAG,
                            "parseSectorAck: INDEX_ERR resync exhausted ($attempt consecutive), abort OTA"
                        )
                        failOtaTransfer()
                        callback?.onError(BleOTAErrors.SECTOR_INDEX_RESYNC_EXHAUSTED)
                        return
                    }
                    Log.w(
                        TAG,
                        "parseSectorAck: INDEX_ERR device expect=$devExpectIndex, resync ($attempt/$MAX_CONSECUTIVE_INDEX_ERR_RESYNC)"
                    )
                    sectorsInFlight = 0
                    rebuildPacketsFromSector(devExpectIndex)
                    callback?.onSectorIndexResync(sectorAckIndex.get())
                    postNextPacket()
                }
                BIN_ACK_PAYLOAD_LENGTH_ERROR -> {
                    callback?.onError(BleOTAErrors.DEVICE_PAYLOAD_LENGTH_ERROR)
                    failOtaTransfer()
                }
                else -> {
                    callback?.onError(BleOTAErrors.UNKNOWN_BIN_ACK_STATUS)
                    failOtaTransfer()
                }
            }
        } catch (e: Exception) {
            Log.w(TAG, "parseSectorAck error", e)
            failOtaTransfer()
            callback?.onError(BleOTAErrors.SECTOR_ACK_PARSE)
        }
    }

    internal fun parseCommandPacketValue(packet: ByteArray) {
        if (DEBUG) {
            Log.i(TAG, "parseCommandPacket: ${packet.contentToString()}")
        }
        if (packet.size < 20) {
            Log.w(TAG, "parseCommandPacket: size=${packet.size}")
            callback?.onError(BleOTAErrors.COMMAND_PACKET_SIZE)
            return
        }
        val crc = u16le(packet, 18)
        val checksum = EspCRC16.crc(packet, 0, 18)
        if (crc != checksum) {
            Log.w(TAG, "parseCommandPacket: Checksum error: $crc, expect $checksum")
            callback?.onError(BleOTAErrors.COMMAND_CHECKSUM)
            return
        }

        val id = u16le(packet, 0)
        if (id != COMMAND_ID_ACK) {
            Log.w(TAG, "parseCommandPacket: unexpected id=0x${id.toString(16)}")
            callback?.onError(BleOTAErrors.COMMAND_NOTIFY_UNEXPECTED_ID)
            return
        }
        val ackId = u16le(packet, 2)
        val ackStatus = u16le(packet, 4)
        val sendWindowByte = packet[6].toInt() and 0xff
        when (ackId) {
            COMMAND_ID_START -> {
                receiveCommandStartAck(ackStatus, sendWindowByte)
            }
            COMMAND_ID_END -> {
                receiveCommandEndAck(ackStatus)
            }
            else -> {
                Log.w(TAG, "parseCommandPacket: unknown ackId=0x${ackId.toString(16)}")
                callback?.onError(BleOTAErrors.UNKNOWN_COMMAND_ACK_ID)
            }
        }
    }

    open class GattCallback : ConnectionObserver {
        var client: BleOTAClient? = null

        override fun onDeviceConnecting(device: BluetoothDevice) {}

        override fun onDeviceConnected(device: BluetoothDevice) {}

        override fun onDeviceFailedToConnect(device: BluetoothDevice, reason: Int) {}

        override fun onDeviceReady(device: BluetoothDevice) {}

        override fun onDeviceDisconnecting(device: BluetoothDevice) {}

        override fun onDeviceDisconnected(device: BluetoothDevice, reason: Int) {
            client?.onDisconnectedDuringTransfer()
        }

        /** Device asked to resend from this sector; default no-op (Activity may show status). */
        open fun onSectorIndexResync(fromSector: Int) {}

        /**
         * Optional UI hooks. [EspBleOtaBleManager] handles GATT internally; override in the app
         * if you extend [BleManager] to forward these.
         */
        open fun onDescriptorWrite(
            gatt: BluetoothGatt,
            descriptor: BluetoothGattDescriptor,
            status: Int
        ) {}

        open fun onCharacteristicWrite(
            gatt: BluetoothGatt,
            characteristic: BluetoothGattCharacteristic,
            status: Int
        ) {}

        /** Called after MTU negotiation during initialization ([success] reflects request outcome). */
        open fun onMtuNegotiated(mtu: Int, success: Boolean) {}

        open fun onError(code: Int) {}

        open fun onOTA(message: BleOTAMessage) {}
    }
}

private fun u16le(data: ByteArray, offset: Int): Int {
    return (data[offset].toInt() and 0xff) or (data[offset + 1].toInt() shl 8 and 0xff00)
}
