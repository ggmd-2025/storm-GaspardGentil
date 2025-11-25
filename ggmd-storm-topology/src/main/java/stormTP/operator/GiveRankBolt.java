package stormTP.operator;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
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
public class GiveRankBolt implements IRichBolt {
    private static final long serialVersionUID = 4262369370788107343L;

	private static Logger logger = Logger.getLogger("GiveRankBoltLogger");
	private OutputCollector collector;

    private final int targetId;
	
	
	/**
	 * Default constructor: filters tortoise with id=3 and gives it a default name.
	 */
	public GiveRankBolt () {  
        this(3);      
    }

	/**
	 * Construct bolt that filters for given tortoise id and assigns a name.
	 * @param targetId tortoise id to filter
	 * @param name name to use for the tortoise when emitting
	 */
	public GiveRankBolt(int targetId) {
        this.targetId = targetId;
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
            List<Integer> poslist = new ArrayList<>(runners.size());
            for (int i = 0; i < runners.size(); ++i) {
                JsonObject runner = runners.getJsonObject(i);
                int id = runner.getInt("id");
                int maxcel = runner.getInt("maxcel");
                int tour = runner.getInt("tour");
                int cellule = runner.getInt("cellule");
                int pos = maxcel * tour + cellule;
            
                poslist.set(id, pos);
            }
            List<Integer> ranks = poslist.stream().sorted().toList();
            for (int i = 0; i < runners.size(); ++i) {
                JsonObject runner = runners.getJsonObject(i);
                int id = runner.getInt("id");
                if( id != this.targetId ) {
                    continue;
                }
                int top = runner.getInt("top");
                int maxcel = runner.getInt("maxcel");
                int total = runner.getInt("total");
                int pos = poslist.get(id);
                int minrank = ranks.indexOf(pos);
                int maxrank = ranks.lastIndexOf(pos);
                String rang = minrank == maxrank ? minrank + "" : minrank + " ex";
                collector.emit(t, new Values(id, top, rang, total, maxcel));
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
		arg0.declare(new Fields("id", "top", "rang", "total", "maxcel"));
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
