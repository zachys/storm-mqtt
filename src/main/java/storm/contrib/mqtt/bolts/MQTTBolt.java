package storm.contrib.mqtt.spouts;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
//import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MQTTSpout extends BaseRichBolt {
	
    //MQTT PARAMETERS
    String topic        = "locationMSG";
    String content      = "Message from MqttPublishSample";
    int qos             = 2;
    String broker       = "tcp://1.2.3.4:1883";     //REPLACE WITH YOUR MQTT IP
    String clientId     = "StormLocationMSGTopology";
    //MemoryPersistence persistence = new MemoryPersistence();

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        config = (Map<String,String>) map;
    }

    @Override
    public void execute(Tuple input) {

        try { 
            final String MENmac = input.getStringByField("MENmac");
            final String x = input.getStringByField("x");
            final String y = input.getStringByField("y");
            content= MENmac+" "+x+" "+y;
             try {
                MqttClient sampleClient = new MqttClient(broker, clientId);
                MqttConnectOptions connOpts = new MqttConnectOptions();
                connOpts.setCleanSession(true);
                sampleClient.connect(connOpts);
                //System.out.println("Connected");
                //System.out.println("Publishing message: "+content);
                MqttMessage message = new MqttMessage(content.getBytes());
                message.setQos(qos);
                sampleClient.publish(topic, message);
                //System.out.println("Message published");
                sampleClient.disconnect();
                //System.out.println("Disconnected");
                //System.exit(0);
            } catch(MqttException me) {
                System.out.println("reason "+me.getReasonCode());
                System.out.println("msg "+me.getMessage());
                System.out.println("loc "+me.getLocalizedMessage());
                System.out.println("cause "+me.getCause());
                System.out.println("excep "+me);
                me.printStackTrace();
        }    
            outputCollector.ack(input);

        } catch (Throwable t) {

            outputCollector.reportError(t);
            outputCollector.fail(input);

        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //FINAL BOLT: NO OUTPUT
        //outputFieldsDeclarer.declare(new Fields(new String[]{"MENmac", "x", "y"}));
    }
	
}
