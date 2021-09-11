const {Kafka} = require('kafkajs');
const faker = require('faker');
const mongoose = require('mongoose');
const Schema = mongoose.Schema;

let conn;
let producer;
let response;
const uri = 'mongodb+srv://dbUser:dbUserPassword@cluster0.3jwdp.mongodb.net/myFirstDatabase?retryWrites=true&w=majority';

exports.lambdaHandler = async (event, context) => {
  try {
    if (conn == null) {
      conn = mongoose.createConnection(uri, {
        serverSelectionTimeoutMS: 5000,
      });
      await conn;
      const EpisodeSchema = new Schema({
        name: String,
      });
      conn.model('Episode', EpisodeSchema);
    }

    const Episode = conn.model('Episode');

    let ep = new Episode();
    ep.name = faker.name.findName();
    await ep.save();

    const read = await Episode.findOne();
    console.log(JSON.stringify(read));

    const kafka = new Kafka({
      clientId: 'sam-app',
      brokers: ['192.168.1.9:9092'],
    });

    producer = kafka.producer();
    await producer.connect();
    console.log('Connected to Kafka');

    await producer.send({
      topic: 'local',
      acks: 1,
      messages: [
        {
          value: JSON.stringify({
            id: 'b312dee2-41cb-4046-aaaa-cbec25f99999',
            name: 'SH20210910',
          }),
        },
      ],
    });

    for (let key in event.records) {
      console.log('Key: ', key);
      event.records[key].map((record) => {
        const msg = Buffer.from(record.value, 'base64').toString();
        console.log('Message:', msg);
      });
    }

    response = {
      'statusCode': 200,
      'body': JSON.stringify({
        message: 'hello world',
      }),
    };
  } catch (err) {
    console.log(err);
    return err;
  }

  return response;
};

