import { kafkaClient } from '.'
import { ELASTIC_TOPIC } from './constant'

const producer = kafkaClient.producer()

export const publishMessage = async (payload) => {
    await producer.connect()
    await producer.send({
        topic: ELASTIC_TOPIC,
        messages: [
            { value: JSON.stringify(payload) }
        ],
    })

    await producer.disconnect()
}