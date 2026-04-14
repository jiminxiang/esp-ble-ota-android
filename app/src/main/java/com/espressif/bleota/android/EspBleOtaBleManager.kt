package com.espressif.bleota.android

import android.bluetooth.BluetoothDevice
import android.bluetooth.BluetoothGatt
import android.bluetooth.BluetoothGattCharacteristic
import android.bluetooth.BluetoothGattService
import android.content.Context
import android.os.Build
import android.util.Log
import no.nordicsemi.android.ble.BleManager
import no.nordicsemi.android.ble.observer.ConnectionObserver

/**
 * Nordic [BleManager] wrapper: queues GATT operations (MTU, notifications, writes) so
 * [BluetoothGatt.writeCharacteristic] is not hammered from app code.
 */
@Suppress("MemberVisibilityCanBePrivate")
internal class EspBleOtaBleManager(
    context: Context,
    private val client: BleOTAClient,
) : BleManager(context.applicationContext) {

    private var recvFw: BluetoothGattCharacteristic? = null
    private var progress: BluetoothGattCharacteristic? = null
    private var command: BluetoothGattCharacteristic? = null
    private var customer: BluetoothGattCharacteristic? = null
    private var otaService: BluetoothGattService? = null

    fun getRecvFwCharacteristic(): BluetoothGattCharacteristic? = recvFw
    fun getProgressCharacteristic(): BluetoothGattCharacteristic? = progress
    fun getCommandCharacteristic(): BluetoothGattCharacteristic? = command
    fun getCustomerCharacteristic(): BluetoothGattCharacteristic? = customer
    fun getOtaService(): BluetoothGattService? = otaService

    override fun log(priority: Int, message: String) {
        if (BleOTAClient.bleDebugLog()) {
            Log.println(priority, TAG, message)
        }
    }

    override fun isRequiredServiceSupported(gatt: BluetoothGatt): Boolean {
        val service = gatt.getService(BleOTAClient.SERVICE_UUID) ?: return false
        otaService = service
        recvFw = service.getCharacteristic(BleOTAClient.CHAR_RECV_FW_UUID)
        progress = service.getCharacteristic(BleOTAClient.CHAR_PROGRESS_UUID)
        command = service.getCharacteristic(BleOTAClient.CHAR_COMMAND_UUID)
        customer = service.getCharacteristic(BleOTAClient.CHAR_CUSTOMER_UUID)
        client.onGattServicesResolved(service, recvFw, progress, command, customer)
        return recvFw != null && progress != null && command != null && customer != null
    }

    override fun initialize() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.LOLLIPOP) {
            requestConnectionPriority(BluetoothGatt.CONNECTION_PRIORITY_HIGH).enqueue()
        }
        requestMtu(BleOTAClient.MTU_SIZE)
            .with { _, mtu ->
                client.applyNegotiatedMtu(mtu)
                client.notifyMtuNegotiated(mtu, success = true)
            }
            .fail { _, _ ->
                client.applyNegotiatedMtu(23)
                client.notifyMtuNegotiated(BleOTAClient.MTU_SIZE, success = false)
            }
            .enqueue()

        recvFw?.let { ch ->
            setNotificationCallback(ch).with { _, data ->
                val v = data.value ?: return@with
                client.parseSectorAck(v)
            }
        }
        progress?.let { ch ->
            setNotificationCallback(ch).with { _, _ -> }
        }
        command?.let { ch ->
            setNotificationCallback(ch).with { _, data ->
                val v = data.value ?: return@with
                client.parseCommandPacketValue(v)
            }
        }
        customer?.let { ch ->
            setNotificationCallback(ch).with { _, _ -> }
        }
    }

    override fun onServicesInvalidated() {
        recvFw = null
        progress = null
        command = null
        customer = null
        otaService = null
        client.onGattServicesInvalidated()
    }

    /** Enable CCC on all OTA notify characteristics, then run [onReady] (e.g. START command). */
    fun enqueueEnableNotificationsThen(onReady: () -> Unit) {
        val rf = recvFw ?: run {
            onReady()
            return
        }
        val pr = progress ?: run {
            onReady()
            return
        }
        val cmd = command ?: run {
            onReady()
            return
        }
        val cust = customer ?: run {
            onReady()
            return
        }
        enableNotifications(rf).enqueue()
        enableNotifications(pr).enqueue()
        enableNotifications(cmd).enqueue()
        enableNotifications(cust)
            .done { onReady() }
            .enqueue()
    }

    fun enqueueCommandWrite(data: ByteArray) {
        val c = command ?: return
        writeCharacteristic(c, data, BluetoothGattCharacteristic.WRITE_TYPE_DEFAULT)
            .split()
            .enqueue()
    }

    fun enqueueFirmwarePacket(
        data: ByteArray,
        onComplete: () -> Unit,
        onFailRequeue: () -> Unit,
    ) {
        val c = recvFw ?: run {
            onComplete()
            return
        }
        writeCharacteristic(c, data, BluetoothGattCharacteristic.WRITE_TYPE_NO_RESPONSE)
            .split()
            .done { onComplete() }
            .fail { _, _ ->
                onFailRequeue()
                onComplete()
            }
            .enqueue()
    }

    fun connectWithObserver(device: BluetoothDevice, observer: ConnectionObserver?) {
        setConnectionObserver(observer)
        connect(device)
            .retry(2, 400)
            .useAutoConnect(false)
            .enqueue()
    }

    fun shutdown() {
        cancelQueue()
        disconnect().enqueue()
    }

    companion object {
        private const val TAG = "EspBleOtaBleManager"
    }
}
