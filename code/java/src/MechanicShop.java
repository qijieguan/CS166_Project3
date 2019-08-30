/*
 * Template JAVA User Interface
 * =============================
 *
 * Database Management Systems
 * Department of Computer Science &amp; Engineering
 * University of California - Riverside
 *
 * Target DBMS: 'Postgres'
 *
 */


import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.ArrayList;

/**
 * This class defines a simple embedded SQL utility class that is designed to
 * work with PostgreSQL JDBC drivers.
 *
 */

public class MechanicShop{
	//reference to physical database connection
	private Connection _connection = null;
	static BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
	
	public MechanicShop(String dbname, String dbport, String user, String passwd) throws SQLException {
		System.out.print("Connecting to database...");
		try{
			// constructs the connection URL
			String url = "jdbc:postgresql://localhost:" + dbport + "/" + dbname;
			System.out.println ("Connection URL: " + url + "\n");
			
			// obtain a physical connection
	        this._connection = DriverManager.getConnection(url, user, passwd);
	        System.out.println("Done");
		}catch(Exception e){
			System.err.println("Error - Unable to Connect to Database: " + e.getMessage());
	        System.out.println("Make sure you started postgres on this machine");
	        System.exit(-1);
		}
	}
	
	/**
	 * Method to execute an update SQL statement.  Update SQL instructions
	 * includes CREATE, INSERT, UPDATE, DELETE, and DROP.
	 * 
	 * @param sql the input SQL string
	 * @throws java.sql.SQLException when update failed
	 * */
	public void executeUpdate (String sql) throws SQLException { 
		// creates a statement object
		Statement stmt = this._connection.createStatement ();

		// issues the update instruction
		stmt.executeUpdate (sql);

		// close the instruction
	    stmt.close ();
	}//end executeUpdate

	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and outputs the results to
	 * standard out.
	 * 
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQueryAndPrintResult (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		/*
		 *  obtains the metadata object for the returned result set.  The metadata
		 *  contains row and column info.
		 */
		ResultSetMetaData rsmd = rs.getMetaData ();
		int numCol = rsmd.getColumnCount ();
		int rowCount = 0;
		
		//iterates through the result set and output them to standard out.
		boolean outputHeader = true;
		while (rs.next()){
			if(outputHeader){
				for(int i = 1; i <= numCol; i++){
					System.out.print(rsmd.getColumnName(i) + "\t");
			    }
			    System.out.println();
			    outputHeader = false;
			}
			for (int i=1; i<=numCol; ++i)
				System.out.print (rs.getString (i) + "\t");
			System.out.println ();
			++rowCount;
		}//end while
		stmt.close ();
		return rowCount;
	}
	
	public int executeQueryAndPrintResult (String query, int k) throws SQLException {
		Statement stmt = this._connection.createStatement ();
		ResultSet rs = stmt.executeQuery (query);
		ResultSetMetaData rsmd = rs.getMetaData ();
		int numCol = rsmd.getColumnCount ();
		int rowCount = 0;
		boolean outputHeader = true;
		while (rs.next()){
			if(outputHeader){
				for(int i = 1; i <= numCol; i++){
					System.out.print(rsmd.getColumnName(i) + "\t");
			    }
			    System.out.println();
			    outputHeader = false;
			}
			for (int i=1; i<=numCol; ++i)
				System.out.print (rs.getString (i) + "\t");
			System.out.println ();
			++rowCount;
			if (k > 0) {
				if (rowCount == k) {
					break;
				}
			}
		}
		stmt.close ();
		return rowCount;
	}
	
	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the results as
	 * a list of records. Each record in turn is a list of attribute values
	 * 
	 * @param query the input query string
	 * @return the query result as a list of records
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public List<List<String>> executeQueryAndReturnResult (String query) throws SQLException { 
		//creates a statement object 
		Statement stmt = this._connection.createStatement (); 
		
		//issues the query instruction 
		ResultSet rs = stmt.executeQuery (query); 
	 
		/*
		 * obtains the metadata object for the returned result set.  The metadata 
		 * contains row and column info. 
		*/ 
		ResultSetMetaData rsmd = rs.getMetaData (); 
		int numCol = rsmd.getColumnCount (); 
		int rowCount = 0; 
	 
		//iterates through the result set and saves the data returned by the query. 
		boolean outputHeader = false;
		List<List<String>> result  = new ArrayList<List<String>>(); 
		while (rs.next()){
			List<String> record = new ArrayList<String>(); 
			for (int i=1; i<=numCol; ++i) 
				record.add(rs.getString (i)); 
			result.add(record); 
		}//end while 
		stmt.close (); 
		return result; 
	}//end executeQueryAndReturnResult
	
	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the number of results
	 * 
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQuery (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		int rowCount = 0;

		//iterates through the result set and count nuber of results.
		if(rs.next()){
			rowCount++;
		}//end while
		stmt.close ();
		return rowCount;
	}
	
	/**
	 * Method to fetch the last value from sequence. This
	 * method issues the query to the DBMS and returns the current 
	 * value of sequence used for autogenerated keys
	 * 
	 * @param sequence name of the DB sequence
	 * @return current value of a sequence
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	
	public int getCurrSeqVal(String sequence) throws SQLException {
		Statement stmt = this._connection.createStatement ();
		
		ResultSet rs = stmt.executeQuery (String.format("Select currval('%s')", sequence));
		if (rs.next()) return rs.getInt(1);
		return -1;
	}

	/**
	 * Method to close the physical connection if it is open.
	 */
	public void cleanup(){
		try{
			if (this._connection != null){
				this._connection.close ();
			}//end if
		}catch (SQLException e){
	         // ignored.
		}//end try
	}//end cleanup

	/**
	 * The main execution method
	 * 
	 * @param args the command line arguments this inclues the <mysql|pgsql> <login file>
	 */
	public static void main (String[] args) {
		if (args.length != 3) {
			System.err.println (
				"Usage: " + "java [-classpath <classpath>] " + MechanicShop.class.getName () +
		            " <dbname> <port> <user>");
			return;
		}//end if
		
		MechanicShop esql = null;
		
		try{
			System.out.println("(1)");
			
			try {
				Class.forName("org.postgresql.Driver");
			}catch(Exception e){

				System.out.println("Where is your PostgreSQL JDBC Driver? " + "Include in your library path!");
				e.printStackTrace();
				return;
			}
			
			System.out.println("(2)");
			String dbname = args[0];
			String dbport = args[1];
			String user = args[2];
			
			esql = new MechanicShop (dbname, dbport, user, "");
			
			boolean keepon = true;
			while(keepon){
				System.out.println("MAIN MENU");
				System.out.println("---------");
				System.out.println("1. AddCustomer");
				System.out.println("2. AddMechanic");
				System.out.println("3. AddCar");
				System.out.println("4. InsertServiceRequest");
				System.out.println("5. CloseServiceRequest");
				System.out.println("6. ListCustomersWithBillLessThan100");
				System.out.println("7. ListCustomersWithMoreThan20Cars");
				System.out.println("8. ListCarsBefore1995With50000Milles");
				System.out.println("9. ListKCarsWithTheMostServices");
				System.out.println("10. ListCustomersInDescendingOrderOfTheirTotalBill");
				System.out.println("11. < EXIT");
				
				/*
				 * FOLLOW THE SPECIFICATION IN THE PROJECT DESCRIPTION
				 */
				switch (readChoice()){
					case 1: AddCustomer(esql); break;
					case 2: AddMechanic(esql); break;
					case 3: AddCar(esql); break;
					case 4: InsertServiceRequest(esql); break;
					case 5: CloseServiceRequest(esql); break;
					case 6: ListCustomersWithBillLessThan100(esql); break;
					case 7: ListCustomersWithMoreThan20Cars(esql); break;
					case 8: ListCarsBefore1995With50000Milles(esql); break;
					case 9: ListKCarsWithTheMostServices(esql); break;
					case 10: ListCustomersInDescendingOrderOfTheirTotalBill(esql); break;
					case 11: keepon = false; break;
				}
			}
		}catch(Exception e){
			System.err.println (e.getMessage ());
		}finally{
			try{
				if(esql != null) {
					System.out.print("Disconnecting from database...");
					esql.cleanup ();
					System.out.println("Done\n\nBye !");
				}//end if				
			}catch(Exception e){
				// ignored.
			}
		}
	}

	public static int readChoice() {
		int input;
		// returns only if a correct value is given.
		do {
			System.out.print("Please make your choice: ");
			try { // read the integer, parse it and break.
				input = Integer.parseInt(in.readLine());
				break;
			}catch (Exception e) {
				System.out.println("Your input is invalid!");
				continue;
			}//end try
		}while (true);
		return input;
	}//end readChoice
	
	public static void AddCustomer(MechanicShop esql)
        {//1
		try{
			/*
			String query = "SELECT * FROM Catalog WHERE cost < ";

			System.out.print("\tEnter cost: $");
			String input = in.readLine();
			query += input;

			int rowCount = esql.executeQuery (query);
			System.out.println ("total row(s): " + rowCount);
			*/
			
			String query = String.format("SELECT MAX(id) FROM Customer");
			List<List<String>> data = esql.executeQueryAndReturnResult(query);
			int id = Integer.parseInt(data.get(0).get(0)) + 1;
			
			System.out.println("\nPlease enter customer's first name: ");
			String fname = in.readLine();
			System.out.println("\nPlease enter customer's last name: ");
			String lname = in.readLine();
			
			query = String.format("SELECT c.* FROM Customer c WHERE c.fname = '%s' AND c.lname = '%s'", fname, lname);
			if (esql.executeQueryAndPrintResult(query) != 0) {
				System.out.println("\nIs this the customer you wish to add (y/n) ");
				String input = in.readLine();
				if (input.equals("y")) {
					throw new RuntimeException("This customer has already been added. Thank you!");
				}
			}
			
			System.out.println("\nPlease enter customer's phone: ");
			String phone = in.readLine();
			System.out.println("\nPlease enter customer's address: ");
			String address = in.readLine();
			
			//Updates Customer query
			query = "INSERT INTO Customer(id, fname, lname, phone, address) VALUES(" + id + ", \'" + fname + "\', \'" + lname + "\', \'" + phone + "\', \'" + address + "\');";
			esql.executeUpdate(query);
			
			query = String.format("SELECT c.* FROM customer c WHERE c.id = %d", id);
			
			int rowCount = esql.executeQueryAndPrintResult (query);
			System.out.println ("total row(s): " + rowCount);
					      
		}catch(Exception e)
	    	{
			System.err.println (e.getMessage ());
	    	}    	

	}
	
	public static void AddMechanic(MechanicShop esql){//2
		try {
			String query = String.format("SELECT MAX(id) FROM Mechanic");
			List<List<String>> data = esql.executeQueryAndReturnResult(query);
			int id = Integer.parseInt(data.get(0).get(0)) + 1;
			
			System.out.println("\nPlease enter mechanic's first name: ");
			String fname = in.readLine();
			System.out.println("\nPlease enter mechanic's last name: ");
			String lname = in.readLine();
			
			query = String.format("SELECT m.* FROM Mechanic m WHERE m.fname = '%s' AND m.lname = '%s'", fname, lname);
			if (esql.executeQueryAndPrintResult(query) != 0) {
				System.out.println("\nIs this the mechanic you wish to add (y/n) ");
				String input = in.readLine();
				if (input.equals("y")) {
					throw new RuntimeException("This mechanic has already been added. Thank you!");
				}
			}
			
			System.out.println("\nPlease enter mechanic's experience: ");
			int experience = Integer.parseInt(in.readLine());
			
			//Updates Mechanic query
			query = "INSERT INTO Mechanic(id, fname, lname, experience) VALUES(" + id + ", \'" + fname + "\', \'" + lname + "\', " + experience + ");";
			esql.executeUpdate(query);
			
			query = String.format("SELECT m.* FROM Mechanic m WHERE m.id = %d", id);
			
			int rowCount = esql.executeQueryAndPrintResult (query);
			System.out.println ("total row(s): " + rowCount);
					      
		}catch(Exception e) {
			System.err.println (e.getMessage ());
		}
	}
	
	public static void AddCar(MechanicShop esql){//3
		try {
			
			//Check if the customer id you enter is valid
			System.out.println("\nPlease enter the Customer id of the Car: ");
			int cust_id = Integer.parseInt(in.readLine());
			String query = String.format("SELECT c.id FROM Customer c WHERE c.id = '%d'", cust_id);
			if (esql.executeQuery(query) == 0) {
				throw new RuntimeException("\nInvalid Customer id.");
			}
			
			System.out.println("\nPlease enter the Car's vin: ");
			String vin = in.readLine();

			//Check if the Car's vin you enter is valid
			query = String.format("SELECT o.* FROM Owns o WHERE o.car_vin = '%s'", vin);
			if (esql.executeQueryAndPrintResult(query) != 0) {
				throw new RuntimeException("\nCar vin already exists.");
			}
			
                        System.out.println("\nPlease enter Car's make: ");
                        String make = in.readLine();
                        System.out.println("\nPlease enter Car's model: ");
                        String model = in.readLine();
                        System.out.println("\nPlease enter Car's year: ");
                        int year = Integer.parseInt(in.readLine());	

			//Updates Car query
                        query = "INSERT INTO Car(vin, make, model, year) VALUES(\'" + vin + "\', \'" + make + "\', \'" + model + "\', " + year + ");";
                        esql.executeUpdate(query);
			
			//Updates Owns query
			query = String.format("SELECT MAX(ownership_id) FROM Owns");
			List<List<String>> data = esql.executeQueryAndReturnResult(query);
			int owner_id = Integer.parseInt(data.get(0).get(0)) + 1;
			
			query = "INSERT INTO Owns(ownership_id, customer_id, car_vin) VALUES(" + owner_id + ", " + cust_id + ", \'" + vin + "\');";
                        esql.executeUpdate(query);

                        query = String.format("SELECT c.* FROM Car c WHERE c.vin = '%s'", vin);

                        int rowCount = esql.executeQueryAndPrintResult(query);
                        System.out.println ("total row(s): " + rowCount);

			
		}catch(Exception e) {
			System.err.println (e.getMessage ());
		}
	}
	
	public static void InsertServiceRequest(MechanicShop esql){//4
		try {
			//Step 1: Enter customer's last name
			String query;
			while(true) {
				System.out.println("\nPlease enter last name of Customer: ");
				String lname = in.readLine();
				query = String.format("SELECT c.id, c.lname, c.fname FROM Customer c WHERE c.lname = '%s'", lname);

				if (esql.executeQuery(query) == 0) {
					System.out.println("\nThere isn't a customer with this last name. Add a Customer (y/n)?");
					String input = in.readLine();
					if (input.equals("y")) {
						AddCustomer(esql);
						System.out.println("\nThis customer doesn't have a car. Please add car");
						AddCar(esql);
						continue;
					}
					else {
						throw new RuntimeException("\nExit Done.");
					}
				}
				else {
					esql.executeQueryAndPrintResult(query);
				}
				break;
			}
			
			//Step 2: Pick the customer by his/her id
			System.out.println("\nPlease select the customer by the id: ");
			int id_input = Integer.parseInt(in.readLine());
			query = String.format("SELECT c.* FROM Customer c WHERE c.id = %d", id_input);
			if (esql.executeQuery(query) == 0) {
				throw new RuntimeException("\nInvalid customer id.");
			}
			esql.executeQueryAndPrintResult(query);
			

			//Step 3: List all the cars of the selected customer
			query = String.format("SELECT c.* FROM Car c, Owns o WHERE c.vin = o.car_vin AND o.customer_id = %d", id_input);
			esql.executeQueryAndPrintResult(query);
	
			
			//Step 4: Select the car for service request
			System.out.println("\nPlease select the car from the list associated with the vin: ");
			String vin_input = in.readLine();
			query = String.format("SELECT c.* FROM Car c WHERE c.vin = '%s'", vin_input);
			esql.executeQueryAndPrintResult(query);
			
			//Step 5: Enter service request information
			query = String.format("SELECT MAX(rid) FROM Service_Request");
			List<List<String>> data = esql.executeQueryAndReturnResult(query);
			int rid = Integer.parseInt(data.get(0).get(0)) + 1;
			
                        System.out.println("\nPlease enter service request's date: ");
                        String date = in.readLine();
                        System.out.println("\nPlease enter service request's odometer: ");
                        int odometer = Integer.parseInt(in.readLine());
			System.out.println("\nPlease enter service request's complaint: ");
                        String complain = in.readLine();
			
			//Updates Service_Request query
			query = "INSERT INTO Service_Request(rid, customer_id, car_vin, date, odometer, complain) VALUES(" + rid + ", " + id_input + ", \'" + vin_input + "\', \'" + date + "\', " + odometer + ", \'" + complain + "\');";
			esql.executeUpdate(query);

                        query = String.format("SELECT s.* FROM Service_Request s WHERE s.rid = %d", rid);

                        int rowCount = esql.executeQueryAndPrintResult(query);
                        System.out.println ("total row(s): " + rowCount);

				
		}catch(Exception e) {
			System.err.println (e.getMessage ());
		}
	}
	
	public static void CloseServiceRequest(MechanicShop esql) throws Exception{//5
		try {
			//Step 1: Enter the service request id
			System.out.println("\nPlease enter the service request id: ");
			int rid = Integer.parseInt(in.readLine());
			System.out.println("\n");
			String query = String.format("SELECT s.* FROM Service_Request s WHERE s.rid = %d", rid);
			if (esql.executeQueryAndPrintResult(query) != 0) {
				System.out.println("\n");
				query = String.format("SELECT c.* FROM Closed_Request c WHERE c.rid = %d", rid);
			        if (esql.executeQueryAndPrintResult(query) == 0) {
				}
				else {
					throw new RuntimeException("\nThis service request is already closed.");
				}
			}
			else {
				throw new RuntimeException("\nService Request does not exist.");
			}
			
			//Step 2: Enter the Mechanic id
			System.out.println("\nPlease enter the Mechanic's id: ");
			int mid = Integer.parseInt(in.readLine());
			System.out.println("\n");
			query = String.format("SELECT m.* FROM Mechanic m WHERE m.id = %d", mid);
			if (esql.executeQueryAndPrintResult(query) != 0) {
			}
			else {
				throw new RuntimeException("\nInvalid Mechanic id.");
			}
			
			//Step 3: Enter closed request information
			query = String.format("SELECT MAX(wid) FROM Closed_Request ");
			List<List<String>> data = esql.executeQueryAndReturnResult(query);
			int wid = Integer.parseInt(data.get(0).get(0)) + 1;
			
			System.out.println("\nPlease enter closed request's date: ");
                        String date = in.readLine();
			System.out.println("\nPlease enter closed request's comment: ");
                        String comment = in.readLine();
			System.out.println("\nPlease enter closed request's bill: ");
                        int bill = Integer.parseInt(in.readLine());
			
			//Updates Closed_Request 
			query = "INSERT INTO Closed_Request(wid, rid, mid, date, comment, bill) VALUES(" + wid + ", " + rid + ", " + mid + ", \'" + date + "\', \'" + comment + "\', " + bill + ");";
			esql.executeUpdate(query);

                        query = String.format("SELECT c.* FROM Closed_Request c WHERE c.wid = %d", wid);

                        int rowCount = esql.executeQueryAndPrintResult(query);
                        System.out.println ("total row(s): " + rowCount);
			
		}catch(Exception e) {
			System.err.println (e.getMessage ());
		}
	}
	
	public static void ListCustomersWithBillLessThan100(MechanicShop esql){//6
		try{
			String query = String.format("SELECT c.fname, c.lname, cr.date, cr.comment, cr.bill FROM Customer c, Service_Request sr, Closed_Request cr WHERE c.id = sr.customer_id AND sr.rid = cr.rid AND cr.bill < 100");
			esql.executeQueryAndPrintResult(query);
		}catch(Exception e) {
			System.err.println (e.getMessage ());
		}
	}
	
	public static void ListCustomersWithMoreThan20Cars(MechanicShop esql){//7
		try {
			String query = String.format("SELECT c.fname AS FirstName, c.lname AS LastName, c2 AS Total_Cars FROM (SELECT Customer_id AS c1, COUNT(customer_id) AS c2 FROM Owns GROUP BY (customer_id)) A, Customer c WHERE A.c1 = c.id AND A.c2 > 20;");
			esql.executeQueryAndPrintResult(query);
		}catch(Exception e) {
			System.err.println (e.getMessage ());
		}
	}
	
	public static void ListCarsBefore1995With50000Milles(MechanicShop esql){//8
		try {
			String query = String.format("SELECT c.make, c.model, c.year, s.odometer FROM Car c, Service_Request s WHERE c.vin = s.car_vin AND c.year < 1995 AND s.odometer < 50000");
			esql.executeQueryAndPrintResult(query);
		}catch(Exception e) {
			System.err.println (e.getMessage ());
		}	
	}
	
	public static void ListKCarsWithTheMostServices(MechanicShop esql){//9
		try {
			System.out.println("\nPlease Enter the K value: ");
			int k = Integer.parseInt(in.readLine());
			String query = String.format("SELECT c.make, c.model, A.c2 AS SR_COUNT FROM (SELECT s.car_vin AS c1, COUNT(s.car_vin) AS c2 FROM Service_Request s GROUP BY (s.car_vin)) A, Car c WHERE c.vin = A.c1 ORDER BY A.c2 DESC;");
			esql.executeQueryAndPrintResult(query, k);
		}catch(Exception e) {
			System.err.println (e.getMessage ());
		}
	}
	
	public static void ListCustomersInDescendingOrderOfTheirTotalBill(MechanicShop esql){//9
		try {
			String query = String.format("SELECT D.fname, D.lname, B.Total_Bill FROM (SELECT sr.customer_id AS A, SUM(cr.bill) AS Total_Bill FROM Closed_Request cr, Service_Request sr, Customer c WHERE sr.rid = cr.rid AND sr.customer_id = c.id GROUP BY sr.customer_id) B, Customer D WHERE D.id = B.A ORDER BY B.Total_Bill DESC;");
			esql.executeQueryAndPrintResult(query);
		}catch(Exception e) {
			System.err.println (e.getMessage ());
		}
	}
}
	
	
	
