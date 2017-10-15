const express = require("express");
const bodyParser = require('body-parser');
const path = require("path");
const app = express();
const kafka = require('kafka-node'),
    Producer = kafka.Producer,
    client = new kafka.Client("localhost:2181"),
    offset = new kafka.Offset(client),
    producer = new Producer(client);


producer.createTopics(['topic3'], true, function (err, data) {});
//View Engine
app.set('view engine', 'ejs');
app.set('views', path.join(__dirname,'views'));

//body parser Middleware
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({extended: false}));

app.use(express.static(path.join(__dirname, 'public')));

app.post('/locationstream/add', function(req, res){
  Consumer = kafka.Consumer,
  consumer = new Consumer(client, [{topic:"topic3", offset:0, partition: 0}], {groupId:"kafka",  autocommit:true}, options={fromOffset:false,  maxNum:1});
  const payloads = [
        { topic: 'topic3', messages: req.body.location, timestamp: Date.now()}
    ];

    consumer.setOffset('topic3', 0, 0);
    producer.send(payloads, function(err, data){
      console.log(data);
    });
    consumer.on('message', function(message){
      console.log(message.value);
    });


  res.render("locationstream", {
    name: req.body.topic,
    hashtag: req.body.location

  });

})

app.listen(4000, function(){
  console.log('Server started on port 4000');
})
