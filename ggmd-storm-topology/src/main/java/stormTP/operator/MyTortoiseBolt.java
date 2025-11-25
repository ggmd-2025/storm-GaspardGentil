package stormTP.operator;

import java.io.StringReader;
import java.util.Map;
import java.util.logging.Logger;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonReader;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;


/**
 * Sample of stateless operator
 * @author gentil
 *
 */
public class MyTortoiseBolt implements IRichBolt {
    private static final long serialVersionUID = 4262369370788107343L;

	private static Logger logger = Logger.getLogger("MyTortoiseBoltLogger");
	private OutputCollector collector;
	private final int targetId;
	private final String name;
	
	
	/**
	 * Default constructor: filters tortoise with id=3 and gives it a default name.
	 */
	public MyTortoiseBolt () {
		this(3, "Caroline");
	}

	/**
	 * Construct bolt that filters for given tortoise id and assigns a name.
	 * @param targetId tortoise id to filter
	 * @param name name to use for the tortoise when emitting
	 */
	public MyTortoiseBolt(int targetId, String name) {
		this.targetId = targetId;
		this.name = name;
	}
	
	/* (non-Javadoc)
	 * @see backtype.storm.topology.IRichBolt#execute(backtype.storm.tuple.Tuple)
	 */
	public void execute(Tuple t) {

		try {
			String n = t.getValueByField("json").toString();
			logger.info("=> " + n + " treated!");
            JsonReader jr = Json.createReader( new StringReader(n) );
			JsonObject obj = jr.readObject();
            
            JsonArray runners = obj.getJsonArray("runners");
            for(int i =0; i<runners.size(); i++) {
                JsonObject runner = runners.getJsonObject(i);

                int id = runner.getInt("id");
                if (id != this.targetId) {
                    continue;
                }
                long top = runner.containsKey("top") ? runner.getJsonNumber("top").longValue() : 0L;
                String nom = runner.containsKey("nom") ? runner.getString("nom") : this.name;
                int cellule = runner.containsKey("cellule") ? runner.getInt("cellule") : 0;
                int tour = obj.containsKey("tour") ? obj.getInt("tour") : 0;
                int total = obj.containsKey("total") ? obj.getInt("total") : 0;

                // compute nbCellsParcourus since start
                int nbCellsParcourus = cellule;
                if (total > 0) {
                    nbCellsParcourus = tour * total + cellule;
                }

                int maxcel = obj.containsKey("maxcel") ? obj.getInt("maxcel") : cellule;
                collector.emit(t, new Values(id, top, nom, nbCellsParcourus, total, maxcel));
                collector.ack(t);
            }
		} catch (Exception e) {
			System.err.println("Empty tuple.");
			
		}
		return;
	}
	
	
	
	/* (non-Javadoc)
	 * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
	 */
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		arg0.declare(new Fields("id", "top", "nom", "nbCellsParcourus", "total", "maxcel"));
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
