package stormTP.operator;


import java.io.StringWriter;
import java.util.Map;
//import java.util.logging.Logger;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonWriter;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import stormTP.stream.StreamEmiter;


public class Exit2Bolt implements IRichBolt {

	private static final long serialVersionUID = 4262369370788107342L;
	//private static Logger logger = Logger.getLogger("ExitBolt");
	private OutputCollector collector;
	int port = -1;
	StreamEmiter semit = null;
	
	public Exit2Bolt (int port) {
		this.port = port;
		this.semit = new StreamEmiter(this.port);
		
	}
	
	/* (non-Javadoc)
	 * @see backtype.storm.topology.IRichBolt#execute(backtype.storm.tuple.Tuple)
	 */
	public void execute(Tuple t) {

		// input schema: (id, top, nom, nbCellsParcourus, total, maxcel)
		try {
			int id = (Integer) t.getValueByField("id");
			long top = ((Number) t.getValueByField("top")).longValue();
			String nom = t.getValueByField("nom").toString();
			int nbCellsParcourus = ((Number) t.getValueByField("nbCellsParcourus")).intValue();
			int total = ((Number) t.getValueByField("total")).intValue();
			int maxcel = ((Number) t.getValueByField("maxcel")).intValue();

			int tour = 0;
			int cellule = nbCellsParcourus;
			if (total > 0) {
				tour = nbCellsParcourus / total; // integer division
				cellule = nbCellsParcourus % total;
			}

			JsonObject json = Json.createObjectBuilder()
				.add("id", id)
				.add("top", top)
				.add("tour", tour)
				.add("cellule", cellule)
				.add("total", total)
				.add("maxcel", maxcel)
				.add("nom", nom)
				.build();
                
			this.semit.send(json.toString());
            System.out.println("salut");
			collector.ack(t);
		} catch (Exception e) {
			collector.fail(t);
		}

		return;
	
	}
	

	
	/* (non-Javadoc)
	 * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
	 */
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		arg0.declare(new Fields("json"));
	}
		

	/* (non-Javadoc)
	 * @see backtype.storm.topology.IComponent#getComponentConfiguration()
	 */
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	/* (non-Javadoc)
	 * @see backtype.storm.topology.IBasicBolt#cleanup()
	 */
	public void cleanup() {
		
	}
	
	/* (non-Javadoc)
	 * @see backtype.storm.topology.IRichBolt#prepare(java.util.Map, backtype.storm.task.TopologyContext, backtype.storm.task.OutputCollector)
	 */
	@SuppressWarnings("rawtypes")
	public void prepare(Map arg0, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}
}