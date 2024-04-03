package ru.quipy.payments.logic

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import ru.quipy.common.utils.NonBlockingOngoingWindow
import ru.quipy.orders.api.OrderPaymentStartedEvent
import ru.quipy.payments.api.PaymentCreatedEvent
import ru.quipy.payments.subscribers.OrderPaymentSubscriber
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicLongArray

@Service
abstract class DefaultPaymentServiceSelector {
    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPaymentSubscriber::class.java)

        fun processingPaymentOperation(
            paymentServices: List<PaymentService>,
            isReset : Boolean,
            getNearest : Int,
            nearestTimes: AtomicLongArray,
            event: OrderPaymentStartedEvent,
            createdEvent: PaymentCreatedEvent,
            ){
            outerCycle@ while(true) {
                if (!isReset) {
                    val index = getNearest
                    if (paymentServices[index].window.putIntoWindow()::class == NonBlockingOngoingWindow.WindowResponse.Success::class) {
                        if (paymentServices[index].rateLimiter.tick()) {
                            try {
                                nearestTimes[index] = (System.currentTimeMillis() + (1.0 / paymentServices[index].calculateSpeed())).toLong()
                                paymentServices[index].submitPaymentRequest(
                                    createdEvent.paymentId,
                                    event.amount,
                                    event.createdAt
                                )
                            } finally {
                                paymentServices[index].window.releaseWindow()
                            }
                            break
                        } else {
                            paymentServices[index].window.releaseWindow()
                        }
                    }
                } else
                    for (index in paymentServices.indices) {
                        if (paymentServices[index].window.putIntoWindow()::class == NonBlockingOngoingWindow.WindowResponse.Success::class) {
                            if (paymentServices[index].rateLimiter.tick()) {
                                try {
                                    nearestTimes[index] = (System.currentTimeMillis() + (1.0 / paymentServices[index].calculateSpeed())).toLong()
                                    paymentServices[index].submitPaymentRequest(
                                        createdEvent.paymentId,
                                        event.amount,
                                        event.createdAt
                                    )
                                } finally {
                                    paymentServices[index].window.releaseWindow()
                                }
                                break@outerCycle
                            } else {
                                paymentServices[index].window.releaseWindow()
                            }
                        }
                    }
            }

        }
    }
}