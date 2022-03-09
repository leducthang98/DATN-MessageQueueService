import { kafkaClient } from '.'
import { ELASTIC_TOPIC } from './constant'

async function run() {
    const kafkaAdmin = kafkaClient.admin()
    await kafkaAdmin.connect()
    await kafkaAdmin.createTopics({
        topics: [
            {
                topic: ELASTIC_TOPIC
            },
            {
                topic: 'default'
            }
        ]
    })
    const topics = await kafkaAdmin.listTopics()
    console.log(topics)
    await kafkaAdmin.disconnect()
}

run()