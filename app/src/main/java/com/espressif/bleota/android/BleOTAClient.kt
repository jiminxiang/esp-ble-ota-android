package com.espressif.bleota.android

import android.annotation.SuppressLint
import android.bluetooth.BluetoothDevice
import android.bluetooth.BluetoothGattCharacteristic
import android.bluetooth.BluetoothGattService
import android.content.Context
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

        internal val SERVICE_UUID = bleUUID("8018")
        internal val CHAR_RECV_FW_UUID = bleUUID("8020")
        internal val CHAR_PROGRESS_UUID = bleUUID("8021")
        internal val CHAR_COMMAND_UUID = bleUUID("8022")
        internal val CHAR_CUSTOMER_UUID = bleUUID("8023")

        private const val REQUIRE_CHECKSUM = false

        /** Max sectors sent ahead without receiving per-sector notify ACK. This value should be modified. */
        private const val SECTOR_SEND_WINDOW = 3

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
    private val sectorAckMark = ByteArray(0)

    /** Sectors fully written (past [sectorAckMark]) but not yet released by BIN_ACK_SUCCESS. */
    private var sectorsInFlight: Int = 0

    fun connect(callback: GattCallback) {
        Log.i(TAG, "start OTA")
        stop()

        this.callback = callback
        callback.client = this
        manager.connectWithObserver(device, callback)
    }

    fun stop() {
        manager.shutdown()
        callback = null

        sectorsInFlight = 0
        packets.clear()
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

    private fun initPackets() {
        sectorAckIndex.set(0)
        sectorsInFlight = 0
        packets.clear()

        val sectors = ArrayList<ByteArray>()
        ByteArrayInputStream(bin).use {
            val buf = ByteArray(4096)
            while (true) {
                val read = it.read(buf)
                if (read == -1) {
                    break
                }
                val sector = buf.copyOf(read)
                sectors.add(sector)
            }
        }
        if (DEBUG) {
            Log.d(TAG, "initPackets: sectors size = ${sectors.size}")
        }

        val block = ByteArray(packetSize - 3)
        for (element in sectors.withIndex()) {
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
            Log.d(TAG, "initPackets: packets size = ${packets.size}")
        }
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

    private fun receiveCommandStartAck(status: Int) {
        Log.i(TAG, "receiveCommandStartAck: status=$status")
        when (status) {
            COMMAND_ACK_ACCEPT -> {
                postBinData()
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
        when (status) {
            COMMAND_ACK_ACCEPT -> {
            }
        }

        val message = EndCommandAckMessage(status)
        callback?.onOTA(message)
    }

    private fun postBinData() {
        thread {
            initPackets()
            postNextPacket()
        }
    }

    private fun postNextPacket() {
        val packet = packets.pollFirst()
        if (packet == null) {
            if (sectorsInFlight == 0) {
                postCommandEnd()
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
                if (sectorsInFlight < SECTOR_SEND_WINDOW) {
                    postNextPacket()
                } else {
                    Log.d(TAG, "postNextPacket: sectors in flight >= SECTOR_SEND_WINDOW, wait for ACK")
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
        try {
            val expectIndex = sectorAckIndex.getAndIncrement()
            val ackIndex = (data[0].toInt() and 0xff) or
                (data[1].toInt() shl 8 and 0xff00)
            if (ackIndex != expectIndex) {
                Log.w(TAG, "takeSectorAck: Receive error index $ackIndex, expect $expectIndex")
                callback?.onError(1)
                return
            }
            val ackStatus = (data[2].toInt() and 0xff) or
                (data[3].toInt() shl 8 and 0xff00)
            Log.d(TAG, "takeSectorAck: index=$ackIndex, status=$ackStatus")
            when (ackStatus) {
                BIN_ACK_SUCCESS -> {
                    sectorsInFlight--
                    if (sectorsInFlight < 0) {
                        Log.w(TAG, "parseSectorAck: sectorsInFlight underflow, clamping to 0")
                        sectorsInFlight = 0
                    }
                    postNextPacket()
                }
                BIN_ACK_CRC_ERROR -> {
                    callback?.onError(2)
                    return
                }
                BIN_ACK_SECTOR_INDEX_ERROR -> {
                    val devExpectIndex = (data[4].toInt() and 0xff) or
                        (data[5].toInt() shl 8 and 0xff00)
                    if (DEBUG) {
                        Log.w(TAG, "parseSectorAck: device expect index = $devExpectIndex")
                    }
                    callback?.onError(3)
                    return
                }
                BIN_ACK_PAYLOAD_LENGTH_ERROR -> {
                    callback?.onError(4)
                    return
                }
                else -> {
                    callback?.onError(5)
                    return
                }
            }
        } catch (e: Exception) {
            Log.w(TAG, "parseSectorAck error")
            if (DEBUG) {
                Log.w(TAG, "parseSectorAck: ", e)
            }
            callback?.onError(-1)
        }
    }

    internal fun parseCommandPacketValue(packet: ByteArray) {
        if (DEBUG) {
            Log.i(TAG, "parseCommandPacket: ${packet.contentToString()}")
        }
        if (REQUIRE_CHECKSUM) {
            val crc = (packet[18].toInt() and 0xff) or (packet[19].toInt() shl 8 and 0xff00)
            val checksum = EspCRC16.crc(packet, 0, 18)
            if (crc != checksum) {
                Log.w(TAG, "parseCommandPacket: Checksum error: $crc, expect $checksum")
                return
            }
        }

        val id = (packet[0].toInt() and 0xff) or (packet[1].toInt() shl 8 and 0xff00)
        if (id == COMMAND_ID_ACK) {
            val ackId = (packet[2].toInt() and 0xff) or
                (packet[3].toInt() shl 8 and 0xff00)
            val ackStatus = (packet[4].toInt() and 0xff) or
                (packet[5].toInt() shl 8 and 0xff00)
            when (ackId) {
                COMMAND_ID_START -> {
                    receiveCommandStartAck(ackStatus)
                }
                COMMAND_ID_END -> {
                    receiveCommandEndAck(ackStatus)
                }
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

        override fun onDeviceDisconnected(device: BluetoothDevice, reason: Int) {}

        /** Called after MTU negotiation during initialization ([success] reflects request outcome). */
        open fun onMtuNegotiated(mtu: Int, success: Boolean) {}

        open fun onError(code: Int) {}

        open fun onOTA(message: BleOTAMessage) {}
    }
}
