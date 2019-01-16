package csi311;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.apache.derby.jdbc.EmbeddedDriver;

import com.fasterxml.jackson.databind.ObjectMapper;

import csi311.MachineSpec.StateTransitions; 

public class FlexOMS {
	
	private Map<String,Order> orders = new HashMap<String,Order>(); 
	private MachineSpec machineSpec; 
	
	private static final String DB_URL = "jdbc:derby:flexoms;create=true";
    private Connection conn = null;
    private Statement stmt = null;

	public FlexOMS() {
	}

	
    public void run(String argSwitch, String argArg) throws Exception {
    	System.out.println("FlexOMS");
        createConnection();
        createTables();
    	if (argSwitch.equals("--state")) {
    		String json = processStateFile(argArg);
       		machineSpec = parseJson(json);
       		dumpMachine(machineSpec);
       		writeMachine(machineSpec.getTenantId(), json); 
    	}
    	else if (argSwitch.equals("--order")) {
    		processOrderFile(argArg); 
    	}
    	else if (argSwitch.equals("--report")) {
    		makeReport(argArg);
    	}
    	else {
    		throw new Exception("Unrecognized switches"); 
    	}
        shutdown();
    }

    
    private void createConnection() {
        try {
            Driver derbyEmbeddedDriver = new EmbeddedDriver();
            DriverManager.registerDriver(derbyEmbeddedDriver);
            conn = DriverManager.getConnection(DB_URL);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    
    private void createTables() {
        try {
        	System.out.println("Checking for table 'statemachines'..."); 
            stmt = conn.createStatement();
            stmt.execute("create table statemachines (" +
            		"tenantId INT not null " + 
            		"CONSTRAINT tenantId_PK PRIMARY KEY," + 
            		"machinespec varchar(8000) not null" + 
                    ")");
            stmt.close();
        	System.out.println("Checking for table 'orders'..."); 
            stmt = conn.createStatement();
            stmt.execute("create table orders (" +
            		"id INT NOT NULL GENERATED ALWAYS AS IDENTITY " + 
            		"CONSTRAINT id_PK PRIMARY KEY," + 
            		"tenantId int not null," + 
            	    "timeMS bigint not null," + 
            		"orderId varchar(12) not null," + 
            		"customerId varchar(9) not null," + 
            		"state varchar(255) not null," + 
            		"description varchar(255) not null," + 
            		"quantity int not null," + 
            		"cost float not null" + 
                    ")");
            stmt.close();
        }
        catch (SQLException sqlExcept) {
        	if(!tableAlreadyExists(sqlExcept)) {
        		sqlExcept.printStackTrace();
        	}
        }
    }
    
    
    private void shutdown() {
        try {
            if (stmt != null) {
                stmt.close();
            }
            if (conn != null) {
                DriverManager.getConnection(DB_URL + ";shutdown=true");
                conn.close();
            }           
        }
        catch (SQLException sqlExcept) {
        }
    }
    
    
    private boolean tableAlreadyExists(SQLException e) {
        boolean exists;
        if(e.getSQLState().equals("X0Y32")) {
            exists = true;
        } else {
            exists = false;
        }
        return exists;
    }
    
    
    
    private String processStateFile(String filename) throws Exception {
    	BufferedReader br = new BufferedReader(new FileReader(filename));  
    	String json = "";
    	String line; 
    	while ((line = br.readLine()) != null) {
    		json += " " + line; 
    	} 
    	br.close();
    	// Get rid of special characters - newlines, tabs.  
    	return json.replaceAll("\n", " ").replaceAll("\t", " ").replaceAll("\r", " "); 
    }

    
    private MachineSpec parseJson(String json) {
        ObjectMapper mapper = new ObjectMapper();
        try { 
        	MachineSpec machineSpec = mapper.readValue(json, MachineSpec.class);
        	return machineSpec; 
        }
        catch (Exception e) {
            e.printStackTrace(); 
        }
        return null;  	
    }
    
    
    private void processOrderFile(String orderFilename) throws Exception {
    	// Open the file and connect it to a buffered reader.
    	BufferedReader br = new BufferedReader(new FileReader(orderFilename));  
    	String line = null;  
    	// Get lines from the file one at a time until there are no more.
    	while ((line = br.readLine()) != null) {
    		processOrder(line);
    	} 
    	// Close the buffer and the underlying file.
    	br.close();
    }
    
    
    private void dumpMachine(MachineSpec machineSpec) {
    	if (machineSpec == null) {
    		return;
    	}
    	System.out.println("tenantId=" + machineSpec.getTenantId());
    	for (StateTransitions st : machineSpec.getMachineSpec()) {
    		System.out.println(st.getState() + " : " + st.getTransitions());
    	}
    }
    
    
    private void processOrder(String line) {	
    	try { 
    		// Parse the line item.
    		String[] tokens = line.split(",");
    		Order order = new Order();  
    		order.setTenantId(Integer.valueOf(tokens[0].trim()));
    		order.setTimeMs(Long.valueOf(tokens[1].trim())); 
    		order.setOrderId(tokens[2].trim());
    		order.setCustomerId(tokens[3].trim());
    		order.setState(tokens[4].trim().toLowerCase());
    		order.setDescription(tokens[5].trim());
    		order.setQuantity(Integer.valueOf(tokens[6].trim())); 
    		order.setCost(Float.valueOf(tokens[7].trim())); 
    		
    		writeOrder(order); 
    	}
    	catch (Exception e) {
    		e.printStackTrace();
    	}
    }
    
    
    private void readMachineSpec(int tenantId) throws Exception { 
        stmt = conn.createStatement();
        String sql = "SELECT machinespec from statemachines where tenantId=" + tenantId;
        ResultSet rs = stmt.executeQuery(sql);
        while (rs.next()){
        	String specStr = rs.getString("machinespec");
        	System.out.println("Spec = " + specStr); 
        	machineSpec = parseJson(specStr); 
        	dumpMachine(machineSpec); 
        }
        rs.close();
        stmt.close();
    }
    
    
    
    private void readOrders(Integer tenantId) throws Exception { 
        stmt = conn.createStatement();
        String sql = "SELECT timeMS,orderId,customerId,state,description,quantity,cost from orders where tenantId=" + tenantId;
        ResultSet rs = stmt.executeQuery(sql);
        while (rs.next()){
        	Order order = new Order();
        	order.setTenantId(tenantId);
        	order.setTimeMs(rs.getLong("timeMS"));
        	order.setOrderId(rs.getString("orderId"));
        	order.setCustomerId(rs.getString("customerId"));
        	order.setState(rs.getString("state"));
        	order.setDescription(rs.getString("description"));
        	order.setQuantity(rs.getInt("quantity"));
        	order.setCost(rs.getFloat("cost"));
        	updateOrder(order); 
        }
        rs.close();
        stmt.close();
    }
    
    
    private void updateOrder(Order newOrder) throws Exception {
    	boolean isNew = false; 
    	if (!orders.containsKey(newOrder.getOrderId())) {
    		orders.put(newOrder.getOrderId(), newOrder);
    		isNew = true; 
    	}
		Order oldOrder = orders.get(newOrder.getOrderId());
		if ( 	(!newOrder.validateOrderFields()) ||  
				(newOrder.getTimeMs() < oldOrder.getTimeMs()) ||
				(!newOrder.getCustomerId().equals(oldOrder.getCustomerId())) || 
				(!MachineSpec.isValidTransition(machineSpec, oldOrder.getState(), newOrder.getState(), isNew)) 
			) {
			System.out.println("Flagging order " + newOrder); 
			oldOrder.setFlagged(true);
		}
		newOrder.setFlagged(oldOrder.isFlagged());
		orders.put(oldOrder.getOrderId(), newOrder);
    }
    

    private void writeOrder(Order order) throws Exception {
    	System.out.println("timeMs = " + order.getTimeMs()); 
        stmt = conn.createStatement();
        stmt.execute("insert into orders (tenantId, timeMs, orderId, customerId, state, description, quantity, cost) values (" +
        		order.getTenantId() + "," + 
        		order.getTimeMs() + "," + 
        		"'" + order.getOrderId() + "'" + "," + 
        		"'" + order.getCustomerId() + "'" + "," + 
        		"'" + order.getState() + "'" + "," + 
        		"'" + order.getDescription() + "'" + "," + 
        		order.getQuantity() + "," + 
        		order.getCost() + 
                ")");
        stmt.close();
    }
    
    
    // Note: this will blow up if you load the same state machine twice.  
    private void writeMachine(int tenantId, String json) throws Exception {
        stmt = conn.createStatement();
        stmt.execute("insert into statemachines (tenantId, machinespec) values (" +
        		tenantId + "," + 
        		"'" + json + "'" +  
                ")");
        stmt.close();
    }
    

    
    private void makeReport(String tenantId) throws Exception {
    	readMachineSpec(Integer.valueOf(tenantId)); 
    	readOrders(Integer.valueOf(tenantId)); 
    	Map<String,Integer> countMap = new HashMap<String,Integer>();
    	Integer countFlagged = 0; 
    	Map<String,Float> valueMap = new HashMap<String,Float>();
    	for (String key : orders.keySet()) {
    		Order o = orders.get(key);
    		if (!countMap.containsKey(o.getState())) {
    			countMap.put(o.getState(), 0);
    			valueMap.put(o.getState(), 0.0f);
    		}
    		if (o.isFlagged()) {
    			countFlagged++; 
    		}
    		else {
    			countMap.put(o.getState(), countMap.get(o.getState()) + 1);
    			valueMap.put(o.getState(), valueMap.get(o.getState()) + o.getCost());
    		}
    	}
    	
    	for (String state : countMap.keySet()) {
    		Float cost = valueMap.get(state);
    		if (cost == null) {
    			cost = 0.0f;
    		}
    		String terminal = "";
    		if (MachineSpec.isTerminalState(machineSpec, state)) {
    			terminal = "(terminal)";
    		}
    		System.out.println(state + " " + countMap.get(state) + " $" + cost + " " + terminal);
    	}
    	System.out.println("flagged " + countFlagged);
    }
    
    
    public static void main(String[] args) {
    	FlexOMS theApp = new FlexOMS();
    	String argSwitch = null;
    	String argArg = null; 
    	if (args.length > 1) {
    		argSwitch = args[0];
    		argArg = args[1]; 
    	}
    	try { 
    		theApp.run(argSwitch, argArg);
    	}
    	catch (Exception e) {
    		System.out.println("Something bad happened!");
    		e.printStackTrace();
    	}
    }	
	
	

}
