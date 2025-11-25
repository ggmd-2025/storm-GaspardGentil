package stormTP.topology;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import stormTP.operator.Exit3Bolt;
import stormTP.operator.InputStreamSpout;
import stormTP.operator.GiveRankBolt;

/**
 * 
 * @author gentil
 * Topologie test permettant d'écouter le Master Input 
 *
 */
public class TopologyT3 {
	
	public static void main(String[] args) throws Exception {
		int nbExecutors = 1;
		int portINPUT = Integer.parseInt(args[0]);
		int portOUTPUT = Integer.parseInt(args[1]);
    	
		/*Création du spout*/
    	InputStreamSpout spout = new InputStreamSpout("127.0.0.1", portINPUT);
    	/*Création de la topologie*/
    	TopologyBuilder builder = new TopologyBuilder();
        /*Affectation à la topologie du spout*/
        builder.setSpout("masterStream", spout);
        /*Affectation à la topologie du bolt qui ne fait rien, il prendra en input le spout localStream*/
        builder.setBolt("ranker", new GiveRankBolt(), nbExecutors).shuffleGrouping("masterStream");
        /*Affectation à la topologie du bolt qui émet le flux de sortie, il prendra en input le bolt nofilter*/
        builder.setBolt("exit", new Exit3Bolt(portOUTPUT), nbExecutors).shuffleGrouping("ranker");
       
        /*Création d'une configuration*/
        Config config = new Config();
        /*La topologie est soumise à STORM*/
        StormSubmitter.submitTopology("topoT3", config, builder.createTopology());
	}
		
	
}