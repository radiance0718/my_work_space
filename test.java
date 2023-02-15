package main;

import main.interfaces.*;

import java.io.*;
import java.sql.*;
import java.util.Properties;
import java.util.*;

import static java.lang.Integer.parseInt;

public class DataManagement implements IDatabaseManipulation {
    private final int maxn = (int)(5e5+5);
    private  String[] item_name = new String[maxn];
    private  String[] item_type = new String[maxn];
    private  String[] r_city = new String[maxn];
    private  String[] r_courier = new String[maxn];
    private  String[] d_city = new String[maxn];
    private  String[] d_courier = new String[maxn];
    private  String[] e_city = new String[maxn];
    private  String[] i_city = new String[maxn];
    private  String[] e_officer = new String[maxn];
    private  String[] i_officer = new String[maxn];
    private  String[] container_type = new String[maxn];
    private  String[] ship_name = new String[maxn];
    private  String[] company_name = new String[maxn];
    private  double[] item_price = new double[maxn];
    private  double[] export_tax = new double[maxn];
    private  double[] import_tax = new double[maxn];
    private  String[] container_code = new String[maxn];
    private  String[] item_state = new String[maxn];

    private  String[] people_type = new String[maxn];
    private  String[] belong_company = new String[maxn];
    private  String[] belong_city = new String[maxn];
    private  String[] gender = new String[maxn];
    private  int[] age = new int[maxn];
    private  String[] phone = new String[maxn];
    private  String[] password = new String[maxn];

    private  String[] citys = new String[maxn<<3];
    private  Map<String, Integer>  cityid = new HashMap<>();
    private  String[] people = new String[maxn<<1];
    private  Map<String, Integer>  peopleid = new HashMap<>();
    private  String[] ships = new String[maxn];
    private  Map<String, Integer>  shipid = new HashMap<>();
    private  Map<String, String>  ship_company = new HashMap<>();
    private  String[] companys = new String[maxn];
    private  Map<String, Integer>  companyid = new HashMap<>();
    private  String[] container = new String[maxn];
    private  Map<String, String>  containertype = new HashMap<>();
    private  String[] item_class = new String[maxn];
    private  Map<String, Integer>  classid = new HashMap<>();
    private  Connection con;

    private final String database;
    private final String rootUser,rootPass;
    private final String SDMUser="sdmuser",SDMPass="sdmuser";
    private final String CourierUser="cuser",CourierPass="cuser";
    private final String CMUser="cmuser",CMPass="cmuser";
    private final String SOUser="souser",SOPass="souser";
    private final String userSeeker="useeker",userSeekerPass="useeker";

    private  int len;
    private  int p;
    private  int cnt = 0;
    private  int cnt_ship = 0, cnt_company = 0, cnt_city = 0, cnt_people = 0, cnt_containter = 0, cnt_class = 0;

    private  String readdata(String s) throws IOException {
        String ret = "";
        while(p < len && s.charAt(p) != '\n' && s.charAt(p) != ','){
            ret += s.charAt(p);
            p++;
        }
        p++;
        return ret;
    }

    private void readfirstline(String s){
        while(p < len &&s.charAt(p) != '\n')p++;
        p++;
    }



    public  String[] unique(String[] a) {
        List<String> list = new ArrayList<>();
        for (int i=0; i<a.length; i++) {
            if(!list.contains(a[i])) {
                list.add(a[i]);
            }
        }

        String[] newArrStr =  list.toArray(new String[1]);
        return newArrStr;
    }

    public  Connection login(String user, String pass){
        Connection c = null;
        try {

            Class.forName("org.postgresql.Driver");
            c = DriverManager
                    .getConnection("jdbc:postgresql://"+database, user, pass);
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        return c;
    }

    public  int getid_company(String s){
        if(s == null || s.equals(""))return 0;
        return companyid.get(s) == null ? 0 : companyid.get(s);
    }

    public  int getid_city(String s){
        if(s == null || s.equals(""))return 0;
        return cityid.get(s) == null ? 0 : cityid.get(s);
    }

    public  int getid_ship(String s){
        if(s == null || s.equals(""))return 0;
        return shipid.get(s) == null ? 0 : shipid.get(s);
    }

    public  int getid_people(String s){
        if(s == null || s.equals(""))return 0;
        return peopleid.get(s) == null ? 0 : peopleid.get(s);
    }

    public  String get_containertype(String s){
        if(s == null || s.equals(""))return "";
        return containertype.get(s) == null ? "" : containertype.get(s);
    }

    public  int getid_class(String s){
        if(s == null)return 0;
        return classid.get(s) == null ? 0 : classid.get(s);
    }


    public  void insert_company() throws SQLException {
        String sql = "insert into companies(company_name)values(?);";
        PreparedStatement ps = con.prepareStatement(sql);
        for(int i = 1;i <= cnt_company;i++){
            ps.setString(1, companys[i]);
            ps.executeUpdate();
        }
    }

    public  void insert_citys() throws SQLException {
        String sql = "insert into cities(city_name)values(?);";
        PreparedStatement ps = con.prepareStatement(sql);
        for(int i = 1;i <= cnt_city;i++){
            ps.setString(1, citys[i]);
            ps.executeUpdate();
        }
    }

    public  void insert_container() throws SQLException {
        String sql = "insert into containers(container_code, container_type)values(?, ?);";
        PreparedStatement ps = con.prepareStatement(sql);
        for(int i = 1;i <= cnt_containter;i++){
            ps.setString(1, container[i]);
            ps.setString(2, get_containertype(container[i]));
            ps.executeUpdate();
        }
    }

    public  void insert_class() throws SQLException {
        String sql = "insert into item_classes(item_class)values(?);";
        PreparedStatement ps = con.prepareStatement(sql);
        for(int i = 1;i <= cnt_class;i++){
            ps.setString(1, item_class[i]);
            ps.executeUpdate();
        }
    }

    public  void insert_ship() throws SQLException {
        String sql = "insert into ships(ship_name, company_id)values(?, ?);";
        PreparedStatement ps = con.prepareStatement(sql);
        for(int i = 1;i <= cnt_ship;i++){
            ps.setString(1, ships[i]);
            int company_id = getid_company(ship_company.get(ships[i]));
            if(company_id == 0) ps.setNull(2, Types.INTEGER);
            else ps.setInt(2, company_id);
            ps.executeUpdate();
        }
    }

    public LogInfo.StaffType get_stafftype(String s){
        if(s.equals("Company Manager"))return LogInfo.StaffType.CompanyManager;
        else if(s.equals("SUSTC Department Manager"))return LogInfo.StaffType.SustcManager;
        else if(s.equals("Courier"))return LogInfo.StaffType.Courier;
        else if(s.equals("Seaport Officer"))return LogInfo.StaffType.SeaportOfficer;
        return LogInfo.StaffType.SeaportOfficer;
    }


    public  void insert_people() throws SQLException {
        String sql = "insert into staffs(name, staff_type, gender, age, city_id, company_id, phone, password)values(?,?::staff_types,?::staff_genders,?,?,?,?,?);";
        PreparedStatement ps = con.prepareStatement(sql);
        for(int i = 1;i <= cnt_people;i++){
            peopleid.put(people[i], i);
            ps.setString(1, people[i]);
            ps.setString(2, people_type[i]);
            ps.setString(3, gender[i]);
            ps.setInt(4, age[i]);

            int city_id = getid_city(belong_city[i]);
            //if(city_id == 0)System.out.println(belong_city[i]+"???");
            if(city_id == 0) ps.setNull(5, Types.INTEGER);
            else ps.setInt(5, city_id);

            int company_id = getid_company(belong_company[i]);
            if(company_id == 0) ps.setNull(6, Types.INTEGER);
            else ps.setInt(6, company_id);

            ps.setString(7, phone[i]);
            ps.setString(8, password[i]);
            ps.executeUpdate();
        }
    }

    public  void insert_maintable() throws SQLException {
        String sql = "insert into records(item_name, item_price, export_tax, import_tax, container_code, item_state, class_id, retrieval_courier_id, delivery_courier_id, export_officer_id, import_officer_id, retrieval_city_id, delivery_city_id, export_city_id, import_city_id, ship_id, company_id)values(?,?,?,?,?,?::item_states,?,?,?,?,?,?,?,?,?,?,?);";
        PreparedStatement ps = con.prepareStatement(sql);
        for(int i = 1;i <= cnt;i++){
            ps.setString(1, item_name[i]);
            ps.setDouble(2, item_price[i]);
            ps.setDouble(3, export_tax[i]);
            ps.setDouble(4, import_tax[i]);
            if((container_code[i] == null) || container_code[i].equals("")) ps.setNull(5, Types.VARCHAR);
            else ps.setString(5, container_code[i]);
            ps.setString(6, item_state[i]);

            int class_id = getid_class(item_type[i]);
            if(class_id != 0) ps.setInt(7, class_id);
            else ps.setNull(7, Types.INTEGER);

            int rcourier_id = getid_people(r_courier[i]);
            if(rcourier_id != 0) ps.setInt(8, rcourier_id);
            else ps.setNull(8, Types.INTEGER);

            int dcourier_id = getid_people(d_courier[i]);
            if(dcourier_id != 0) ps.setInt(9, dcourier_id);
            else ps.setNull(9, Types.INTEGER);

            int eofficer_id = getid_people(e_officer[i]);
            if(eofficer_id != 0) ps.setInt(10, eofficer_id);
            else ps.setNull(10, Types.INTEGER);

            int iofficer_id = getid_people(i_officer[i]);
            if(iofficer_id != 0) ps.setInt(11, iofficer_id);
            else ps.setNull(11, Types.INTEGER);

            int rcity_id = getid_city(r_city[i]);
            if(rcity_id != 0) ps.setInt(12, rcity_id);
            else ps.setNull(12, Types.INTEGER);

            int dcity_id = getid_city(d_city[i]);
            if(dcity_id != 0) ps.setInt(13, dcity_id);
            else ps.setNull(13, Types.INTEGER);

            int ecity_id = getid_city(e_city[i]);
            if(ecity_id != 0) ps.setInt(14, ecity_id);
            else ps.setNull(14, Types.INTEGER);

            int icity_id = getid_city(i_city[i]);
            if(icity_id != 0) ps.setInt(15, icity_id);
            else ps.setNull(15, Types.INTEGER);


            int ship_id = getid_ship(ship_name[i]);
            if(ship_id != 0) ps.setInt(16, ship_id);
            else ps.setNull(16, Types.INTEGER);

            int company_id = getid_company(company_name[i]);
            if(company_id != 0) ps.setInt(17, company_id);
            else ps.setNull(17, Types.INTEGER);
            ps.executeUpdate();
        }
    }
    public  void pri(){
        System.out.println(cnt);
        for(int i = cnt-100;i <= cnt;i++){
            System.out.println(i);
            System.out.println(item_name[i]);
            System.out.println(item_type[i]);
            System.out.println(item_price[i]);
            System.out.println(r_city[i]);
            System.out.println(r_courier[i]);
            System.out.println(d_city[i]);
            System.out.println(d_courier[i]);
            System.out.println(e_city[i]);

            System.out.println(export_tax[i]);
            System.out.println(e_officer[i]);
            System.out.println(ship_name[i]);
            System.out.println(item_state[i]);
        }

        for(int i = 1;i <= cnt_people;i++){
            System.out.println(people[i]);
            System.out.println(password[i]);
        }
    }


    public  void insert(String recordsCSV, String StaffsCSV) throws SQLException, IOException {
        len = recordsCSV.length();
        p = 0;
        readfirstline(recordsCSV);
        while(p < len-1){
            cnt++;
            item_name[cnt] = readdata(recordsCSV);
            item_type[cnt] = readdata(recordsCSV);
            if(!item_type[cnt].equals(""))item_class[++cnt_class] = item_type[cnt];
            item_price[cnt] = parseInt(readdata(recordsCSV));
            r_city[cnt] = readdata(recordsCSV);
            if(!r_city[cnt].equals(""))citys[++cnt_city] = r_city[cnt];
            r_courier[cnt] = readdata(recordsCSV);
            d_city[cnt] = readdata(recordsCSV);
            if(!d_city[cnt].equals(""))citys[++cnt_city] = d_city[cnt];
            d_courier[cnt] = readdata(recordsCSV);
            e_city[cnt] = readdata(recordsCSV);
            if(!e_city[cnt].equals(""))citys[++cnt_city] = e_city[cnt];
            i_city[cnt] = readdata(recordsCSV);
            if(!i_city[cnt].equals(""))citys[++cnt_city] = i_city[cnt];
            export_tax[cnt] = Double.parseDouble(readdata(recordsCSV));
            import_tax[cnt] = Double.parseDouble(readdata(recordsCSV));
            e_officer[cnt] = readdata(recordsCSV);
            i_officer[cnt] = readdata(recordsCSV);
            container_code[cnt] = readdata(recordsCSV);
            container_type[cnt] = readdata(recordsCSV);
            if(!container_code[cnt].equals(""))container[++cnt_containter] = container_code[cnt];
            if(!container_code[cnt].equals(""))containertype.put(container_code[cnt], container_type[cnt]);
            ship_name[cnt] = readdata(recordsCSV);
            if(!ship_name[cnt].equals(""))ships[++cnt_ship] = ship_name[cnt];
            company_name[cnt] = readdata(recordsCSV);
            if(!company_name[cnt].equals(""))companys[++cnt_company] = company_name[cnt];
            item_state[cnt] = readdata(recordsCSV);
            ship_company.put(ship_name[cnt], company_name[cnt]);

        }
        //pri();
        p = 0;
        len = StaffsCSV.length();
        readfirstline(StaffsCSV);
        while(p < len-1){
            ++cnt_people;
            people[cnt_people] = readdata(StaffsCSV);
            people_type[cnt_people] = readdata(StaffsCSV);
            belong_company[cnt_people] = readdata(StaffsCSV);
            belong_city[cnt_people] = readdata(StaffsCSV);
            if(!belong_city[cnt_people].equals(""))citys[++cnt_city] = belong_city[cnt_people];
            gender[cnt_people] = readdata(StaffsCSV);
            age[cnt_people] = parseInt(readdata(StaffsCSV));
            phone[cnt_people] = readdata(StaffsCSV);
            password[cnt_people] = readdata(StaffsCSV);
        }

        citys = unique(citys);
        cnt_city = citys.length-1;
        for(int i = 1;i <= cnt_city;i++){
            cityid.put(citys[i], i);
        }

        ships = unique(ships);
        cnt_ship = ships.length-1;
        for(int i = 1;i <= cnt_ship;i++){
            shipid.put(ships[i], i);
        }
//		for(int i = 1;i <= cnt_ship;i++){
//			System.out.println(ships[i]);
//		}

        companys = unique(companys);
        cnt_company = companys.length-1;
        for(int i = 1;i <= cnt_company;i++){
            companyid.put(companys[i], i);
        }

        container = unique(container);
        cnt_containter = container.length-1;

        item_class = unique(item_class);
        cnt_class = item_class.length-1;
        for(int i = 1;i <= cnt_class;i++){
            classid.put(item_class[i], i);
        }


        con = login(rootUser, rootPass);
        insert_company();
        insert_citys();
        insert_ship();
        insert_people();
        insert_class();
        insert_container();
        insert_maintable();
        con.close();

        System.out.println("Insertion complete.");

    }


    @Override
    public void $import(String s, String s1) {
        try {
            //System.out.println(s);
            System.out.println("Inserting, please wait...");
            insert(s, s1);

        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }
    }
    public String readFile(String path) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(path));
        StringBuilder sb = new StringBuilder();
        reader.lines().forEach(l -> {
            sb.append(l);
            sb.append("\n");
        });
        return sb.toString();
    }


    public DataManagement(String database, String root, String pass) throws ClassNotFoundException, SQLException {
        this.database=database;
        rootUser=root;
        rootPass=pass;
        Class.forName("org.postgresql.Driver");
        Driver driver=new org.postgresql.Driver();
        Properties prop=new Properties();
        prop.setProperty("user", root);
        prop.setProperty("password", pass);
        Connection conn=driver.connect("jdbc:postgresql://"+database,prop);
        Statement statement=conn.createStatement();
        statement.executeUpdate("""
                drop table if exists records;
                drop table if exists staffs;
                drop table if exists ships;
                drop table if exists cities;
                drop table if exists item_classes;
                drop table if exists containers;
                drop table if exists companies;
                drop type if exists staff_types;
                drop type if exists staff_genders;
                drop type if exists item_states;
                
                
                create table if not exists cities(
                  city_id integer primary key generated always as identity,
                  city_name varchar not null
                );
                
                create table if not exists item_classes(
                  item_class_id integer primary key generated always as identity,
                  item_class varchar not null
                );
                
                create table if not exists containers(
                  container_code varchar primary key,
                  container_type varchar not null
                );
                
                create table if not exists companies(
                  company_id integer primary key generated always as identity,
                  company_name varchar not null
                );
                
                create table if not exists ships(
                  ship_id integer primary key generated always as identity,
                  ship_name varchar not null,
                  company_id integer references companies(company_id) not null
                );
                
                create type staff_types as enum ('Courier','Company Manager','Seaport Officer','SUSTC Department Manager');
                
                create type staff_genders as enum('male','female');
                
                create table if not exists staffs(
                  staff_id integer primary key generated always as identity,
                  name varchar not null,
                  staff_type staff_types not null,
                  company_id integer references companies(company_id),
                  city_id integer references cities(city_id),
                  gender staff_genders not null,
                  age integer not null,
                  phone varchar not null,
                  password varchar not null
                );
                
                create type item_states as enum(
                  'Start',
                  'Picking-up',
                  'To-Export Transporting',
                  'Export Checking',
                  'Export Check Fail',
                  'Packing to Container',
                  'Waiting for Shipping',
                  'Shipping',
                  'Unpacking from Container',
                  'Import Checking',
                  'Import Check Fail',
                  'From-Import Transporting',
                  'Delivering',
                  'Finish'
                );
                
                create table if not exists records(
                  item_name varchar primary key,
                  class_id integer references item_classes(item_class_id) not null,
                  item_price integer not null,
                  retrieval_city_id integer references cities(city_id) not null,
                  retrieval_courier_id integer references staffs(staff_id) not null,
                  delivery_city_id integer references cities(city_id) not null,
                  delivery_courier_id integer references staffs(staff_id),
                  export_city_id integer references cities(city_id) not null,
                  import_city_id integer references cities(city_id) not null,
                  export_tax numeric not null,
                  import_tax numeric not null,
                  export_officer_id integer references staffs(staff_id),
                  import_officer_id integer references staffs(staff_id),
                  container_code varchar references containers(container_code),
                  ship_id integer references ships(ship_id),
                  company_id integer references companies(company_id),
                  item_state item_states not null
                );
                
                
                drop user if exists useeker;
                drop user if exists sdmuser;
                drop user if exists cuser;
                drop user if exists cmuser;
                drop user if exists souser;
                
                create user useeker with password 'useeker';
                create user sdmuser with password 'sdmuser';
                create user cuser with password 'cuser';
                create user cmuser with password 'cmuser';
                create user souser with password 'souser';
                
                grant select on staffs to useeker;
                
                grant select on records to sdmuser;
                grant select on staffs to sdmuser;
                grant select on ships to sdmuser;
                grant select on cities to sdmuser;
                grant select on companies to sdmuser;
                grant select on item_classes to sdmuser;
                grant select on containers to sdmuser;
                grant select on companies to sdmuser;
                
                grant insert on records to cuser;
                grant update on records to cuser;
                grant select on records to cuser;
                grant select on cities to cuser;
                grant select on staffs to cuser;
                 
                grant select on cities to cmuser;
                grant select on ships to cmuser;
                grant select on records to cmuser;
                grant select on item_classes to cmuser;
                grant update on records to cmuser;
                grant insert on ships to cmuser;
                
                
                grant select on records to souser;
                grant update on records to souser;
                grant select on staffs to souser;
                """);
        conn.close();
    }

    private String getType(LogInfo log){
        return switch (log.type()) {
            case Courier -> "Courier";
            case SustcManager -> "SUSTC Department Manager";
            case CompanyManager -> "Company Manager";
            case SeaportOfficer -> "Seaport Officer";
        };
    }

    private boolean userLogin(LogInfo log, LogInfo.StaffType type){
        try{
            Class.forName("org.postgresql.Driver");
            Driver driver=new org.postgresql.Driver();
            Properties prop=new Properties();
            prop.setProperty("user", userSeeker);
            prop.setProperty("password", userSeekerPass);
            Connection conn=driver.connect("jdbc:postgresql://"+database,prop);
            Statement statement=conn.createStatement();
            ResultSet userResultSet=statement.executeQuery("""
                    select *
                    from staffs
                    where name='"""+log.name()+"' and password='"+log.password()+"' and staff_type='"+getType(log)+"'");
            if(!userResultSet.next()||log.type()!=type){
                conn.close();
                return false;
            }
            conn.close();
            return true;
        }catch(Exception e){
            return false;
        }
    }

    @Override
    public double getImportTaxRate(LogInfo log, String city, String itemClass) {
        try{
            if(!userLogin(log, LogInfo.StaffType.CompanyManager)){
                return -1.0;
            }
            Class.forName("org.postgresql.Driver");
            Driver driver=new org.postgresql.Driver();
            Properties prop=new Properties();
            prop.setProperty("user", CMUser);
            prop.setProperty("password", CMPass);
            Connection conn=driver.connect("jdbc:postgresql://"+database,prop);
            Statement statement=conn.createStatement();

            double taxSum=0;
            long priceSum=0;
            ResultSet resultSet=statement.executeQuery("""
                    select item_price,import_tax
                    from (
                        select item_price,item_class,import_tax,city_name
                        from records
                        left join cities c on c.city_id = records.import_city_id
                        left join item_classes ic on records.class_id = ic.item_class_id
                        ) as tmp
                    where city_name='"""+city+"' and item_class='"+itemClass+"'");
            while(resultSet.next()){
                taxSum+=resultSet.getDouble("import_tax");
                priceSum+=resultSet.getInt("item_price");
            }
            conn.close();
            return priceSum==0?-1.0:taxSum/priceSum;
        }
        catch(Exception e){
            return -1.0;
        }
    }

    @Override
    public double getExportTaxRate(LogInfo log, String city, String itemClass) {
        try{
            if(!userLogin(log, LogInfo.StaffType.CompanyManager)){
                return -1.0;
            }
            Class.forName("org.postgresql.Driver");
            Driver driver=new org.postgresql.Driver();
            Properties prop=new Properties();
            prop.setProperty("user", CMUser);
            prop.setProperty("password", CMPass);
            Connection conn=driver.connect("jdbc:postgresql://"+database,prop);
            Statement statement=conn.createStatement();

            double taxSum=0;
            long priceSum=0;
            ResultSet resultSet=statement.executeQuery("""
                    select item_price,export_tax
                    from (
                        select item_price,item_class,export_tax,city_name
                        from records
                        left join cities c on c.city_id = records.export_city_id
                        left join item_classes ic on records.class_id = ic.item_class_id
                        ) as tmp
                    where city_name='"""+city+"' and item_class='"+itemClass+"'");
            while(resultSet.next()){
                taxSum+=resultSet.getDouble("export_tax");
                priceSum+=resultSet.getInt("item_price");
            }
            conn.close();
            return priceSum==0?-1.0:taxSum/priceSum;
        }
        catch(Exception e){
            return -1.0;
        }
    }

    @Override
    public boolean loadItemToContainer(LogInfo log, String itemName, String containerCode) {
        try{
            if(!userLogin(log, LogInfo.StaffType.CompanyManager)){
                return false;
            }
            Class.forName("org.postgresql.Driver");
            Driver driver=new org.postgresql.Driver();
            Properties prop=new Properties();
            prop.setProperty("user", CMUser);
            prop.setProperty("password", CMPass);
            Connection conn=driver.connect("jdbc:postgresql://"+database,prop);
            Statement statement=conn.createStatement();
            ResultSet resultSet= statement.executeQuery("""
                    select *
                    from records
                    where item_name='"""+itemName+"'");
            if(!resultSet.next()){
                return false;
            }
            String item_state=resultSet.getString("item_state");
            if(!item_state.equals("Packing to Container")){
                return false;
            }
            resultSet= statement.executeQuery("""
                    select *
                    from records
                    where container_code = '"""+containerCode+"'");
            if(!resultSet.next()){
                return false;
            }
            statement.executeUpdate("update records\n"+
                    "set container_code='"+containerCode+"'\n"+
                    "where item_name = '" +itemName+"'"
            );
            conn.close();
            return true;
        }
        catch(Exception e){
            return false;
        }
    }

    @Override
    public boolean loadContainerToShip(LogInfo log, String shipName, String containerCode) {
        try{
            if(!userLogin(log, LogInfo.StaffType.CompanyManager)){
                return false;
            }
            Class.forName("org.postgresql.Driver");
            Driver driver=new org.postgresql.Driver();
            Properties prop=new Properties();
            prop.setProperty("user", CMUser);
            prop.setProperty("password", CMPass);
            Connection conn=driver.connect("jdbc:postgresql://"+database,prop);
            Statement statement=conn.createStatement();
            ResultSet resultSet=statement.executeQuery("""
                    select *
                    from records
                    where container_code='"""+containerCode+"' and item_state='Packing to Container'");
            if(!resultSet.next()){
                return false;
            }
            int companyId = resultSet.getInt("company_id");
            resultSet=statement.executeQuery("""
                    select *
                    from ships
                    where ship_name='"""+shipName+"'");
            int shipId = 0;
            if(!resultSet.next()){
                return false;
            }else{
                if(companyId!=resultSet.getInt("company_id")){
                    return false;
                }
            }
            shipId=resultSet.getInt("ship_id");
            resultSet= statement.executeQuery("""
                    select *
                    from records
                    where ship_id="""+shipId+" and item_state='Shipping'");
            if(resultSet.next()){
                return false;
            }
            statement.executeUpdate("""
                    update records
                    set item_state='Waiting for Shipping'
                    where container_code='"""+containerCode+"'");
            statement.executeUpdate("""
                    update records
                    set ship_id = """ +shipId+"\n"+
                    "where container_code='"+containerCode+"'");
            conn.close();

            return true;
        }
        catch(Exception e){
            return false;
        }
    }

    @Override
    public boolean shipStartSailing(LogInfo log, String shipName) {
        try{
            if(!userLogin(log, LogInfo.StaffType.CompanyManager)){
                return false;
            }
            Class.forName("org.postgresql.Driver");
            Driver driver=new org.postgresql.Driver();
            Properties prop=new Properties();
            prop.setProperty("user", CMUser);
            prop.setProperty("password", CMPass);
            Connection conn=driver.connect("jdbc:postgresql://"+database,prop);
            Statement statement=conn.createStatement();
            ResultSet resultSet= statement.executeQuery("""
                    select item_name
                    from records
                    left join ships s on records.ship_id = s.ship_id
                    where ship_name='"""+shipName+"' and item_state='Waiting for Shipping'");
            ArrayList<String> modify=new ArrayList<>();
            while(resultSet.next()){
                modify.add(resultSet.getString("item_name"));
            }
            if(modify.isEmpty()){
                conn.close();
                return false;
            }
            for(String s:modify){
                statement.executeUpdate("""
                        update records
                        set item_state='Shipping'
                        where item_name='"""+s+"'");
            }
            conn.close();
            return true;
        }
        catch(Exception e){
            return false;
        }
    }

    @Override
    public boolean unloadItem(LogInfo log, String itemName) {
        try{
            if(!userLogin(log, LogInfo.StaffType.CompanyManager)){
                return false;
            }
            Class.forName("org.postgresql.Driver");
            Driver driver=new org.postgresql.Driver();
            Properties prop=new Properties();
            prop.setProperty("user", CMUser);
            prop.setProperty("password", CMPass);
            Connection conn=driver.connect("jdbc:postgresql://"+database,prop);
            Statement statement=conn.createStatement();
            ResultSet resultSet=statement.executeQuery("""
                    select *
                    from records
                    where item_name='"""+itemName+"'");
            if(!resultSet.next()){
                return false;
            }
            if(!resultSet.getString("item_state").equals("Shipping")){
                return false;
            }
            statement.executeUpdate("""
                    update records
                    set item_state='Unpacking from Container'
                    where item_name='"""+itemName+"'");
            conn.close();
            return true;
        }
        catch(Exception e){
            return false;
        }
    }

    @Override
    public boolean itemWaitForChecking(LogInfo log, String item) {
        try{
            if(!userLogin(log, LogInfo.StaffType.CompanyManager)){
                return false;
            }
            Class.forName("org.postgresql.Driver");
            Driver driver=new org.postgresql.Driver();
            Properties prop=new Properties();
            prop.setProperty("user", CMUser);
            prop.setProperty("password", CMPass);
            Connection conn=driver.connect("jdbc:postgresql://"+database,prop);
            Statement statement=conn.createStatement();
            ResultSet resultSet= statement.executeQuery("""
                    select *
                    from records
                    where item_name='"""+item+"'");
            if(!resultSet.next()){
                return false;
            }
            if(!resultSet.getString("item_state").equals("Unpacking from Container")){
                return false;
            }
            statement.executeUpdate("""
                    update records
                    set item_state='Import Checking', container_code=null
                    where item_name='"""+item+"'");
            conn.close();
            return true;
        }
        catch(Exception e){
            return false;
        }
    }

    public boolean check_equal(double a, double b){
        double bit = 0.0001;
        if(a - bit <= b && b <= a + bit)return true;
        else return false;
    }

    @Override
    public boolean newItem(LogInfo logInfo, ItemInfo itemInfo){
        try {
            if(itemInfo.name() == null)return false;
            if (!userLogin(logInfo, LogInfo.StaffType.Courier)) return false;
            if (getid_city(itemInfo.retrieval().city()) == 0) return false;
            if (getid_city(itemInfo.delivery().city()) == 0) return false;
            if (getid_city(itemInfo.export().city()) == 0) return false;
            if (getid_city(itemInfo.$import().city()) == 0) return false;
            if (getid_class(itemInfo.$class()) == 0) return false;
            if (itemInfo.price() <= 0) return false;
            if (itemInfo.state() != null && !itemInfo.state().equals(ItemState.PickingUp) ) {
                return false;
            }

            if (!(itemInfo.retrieval().courier() == null) && !(itemInfo.retrieval().courier().equals(logInfo.name()))) return false;
            if (!(itemInfo.delivery().courier() == null)) return false;
            if (!(itemInfo.export().officer() == null)) return false;
            if (!(itemInfo.$import().officer() == null)) return false;
            if (itemInfo.retrieval().city().equals(itemInfo.delivery().city())) return false;
            if (itemInfo.export().city().equals(itemInfo.$import().city())) return false;
            if (itemInfo.export().city().equals(itemInfo.delivery().city())) return false;
            if (itemInfo.$import().city().equals(itemInfo.retrieval().city())) return false;
            String name = logInfo.name();
            Class.forName("org.postgresql.Driver");
            Driver driver = new org.postgresql.Driver();
            Properties prop = new Properties();
            prop.setProperty("user", CourierUser);
            prop.setProperty("password", CourierPass);
            Connection conn = driver.connect("jdbc:postgresql://" + database, prop);
            Statement statement = conn.createStatement();
            ResultSet where_res = statement.executeQuery("""
                    select city_name
                    from staffs inner join cities c on c.city_id = staffs.city_id
                    where name = '""" + name + "';"
            );
            if(!where_res.next())return false;
            String where = where_res.getString("city_name");
            if (!where.equals(itemInfo.retrieval().city())) return false;
            double rate_export = itemInfo.export().tax() / itemInfo.price();
            ResultSet rate_export_res = statement.executeQuery("""
                    select (export_tax/item_price) as export_tax
                    from records
                    where export_city_id = """ + getid_city(itemInfo.export().city()) + "and class_id = " + getid_class(itemInfo.$class())
            );
            if(!rate_export_res.next())return false;

            double rate_export_check = Double.parseDouble(rate_export_res.getString(1));
            if (!check_equal(rate_export_check, rate_export)) return false;
            double rate_import = itemInfo.$import().tax() / itemInfo.price();
            ResultSet rate_import_res = statement.executeQuery("""
                    select (import_tax/item_price) as export_tax
                    from records
                    where import_city_id = """ + getid_city(itemInfo.$import().city()) + "and class_id = " + getid_class(itemInfo.$class())
            );
            if(!rate_import_res.next())return false;
            double rate_import_check = Double.parseDouble(rate_import_res.getString(1));
            if (!check_equal(rate_import_check, rate_import)) return false;
            ResultSet company_res = statement.executeQuery("""
                    select company_id
                    from staffs
                    where name = '""" + logInfo.name() + "';"
            );
            if(!company_res.next())return false;
            int company_id = company_res.getInt(1);

            statement.executeUpdate("""
                    insert into records(
                    item_name, 
                    item_price, 
                    export_tax, 
                    import_tax, 
                    item_state, 
                    class_id, 
                    retrieval_courier_id, 
                    retrieval_city_id, 
                    delivery_city_id, 
                    export_city_id, 
                    import_city_id, 
                    company_id)
                    values(
                    '""" + itemInfo.name() + "',"
                    + itemInfo.price() + ","
                    + itemInfo.export().tax() + ","
                    + itemInfo.$import().tax() + ","
                    + "'Picking-up',"
                    + getid_class(itemInfo.$class()) + ","
                    + getid_people(logInfo.name()) + ","
                    + getid_city(itemInfo.retrieval().city()) + ","
                    + getid_city(itemInfo.delivery().city()) + ","
                    + getid_city(itemInfo.export().city()) + ","
                    + getid_city(itemInfo.$import().city()) + ","
                    + company_id
                    +")"
            );
            conn.close();
            return true;
        }
        catch(Exception e){
            return false;
        }
    }

    @Override
    public boolean setItemState (LogInfo logInfo, String s, ItemState itemState){
        try {
            if (!userLogin(logInfo, LogInfo.StaffType.Courier)) return false;
            Class.forName("org.postgresql.Driver");
            Driver driver = new org.postgresql.Driver();
            Properties prop = new Properties();
            prop.setProperty("user", CourierUser);
            prop.setProperty("password", CourierPass);
            Connection conn = driver.connect("jdbc:postgresql://" + database, prop);
            Statement statement = conn.createStatement();
            ResultSet exist = statement.executeQuery("""
                    select item_name, item_state
                    from records
                    where item_name = '""" + s + "'"
            );
            if(!exist.next())return false;
            if (exist.getString(1).equals("")) return false;
            String last_state = exist.getString(2);
            if (last_state.equals("Picking-up")) {
                if (!itemState.equals(ItemState.ToExportTransporting)) return false;
                ResultSet who_res = statement.executeQuery("""
                        select name
                        from records inner join staffs s
                        on records.retrieval_courier_id = s.staff_id
                        where item_name = '""" + s + "'"
                );
                if(!who_res.next())return false;
                String who = who_res.getString(1);
                if (!who.equals(logInfo.name())) return false;
                statement.executeUpdate("""
                        update records
                        set item_state = 'To-Export Transporting'
                        where item_name = '""" + s + "'"
                );
            } else if (last_state.equals("To-Export Transporting")) {
                if (!itemState.equals(ItemState.ExportChecking)) return false;
                ResultSet who_res = statement.executeQuery("""
                        select name
                        from records inner join staffs s
                        on records.retrieval_courier_id = s.staff_id
                        where item_name = '""" + s + "'"
                );
                String who = who_res.getString(1);
                if (!who.equals(logInfo.name())) return false;

                statement.executeUpdate("""
                        update records
                        set item_state = 'Export Checking'
                        where item_name = '""" + s + "'"
                );
            } else if (last_state.equals("From-Import Transporting")) {
                ResultSet who_res = statement.executeQuery("""
                        select name
                        from records inner join staffs s
                        on records.delivery_courier_id = s.staff_id
                        where item_name = '""" + s + "'"
                );
                if(!who_res.next())return false;
                String who = who_res.getString(1);
                if (!who.equals(logInfo.name()) && !who.equals("")) return false;

                if (who.equals("")) {
                    if (!itemState.equals(ItemState.FromImportTransporting)) return false;

                    ResultSet where_res = statement.executeQuery("""
                            select import_city_id
                            from records
                            where item_name = '""" + s + "';"
                    );
                    if(!where_res.next())return false;
                    int where_id = where_res.getInt(1);

                    ResultSet staff_city_res = statement.executeQuery("""
                            select city_id
                            from staffs
                            where name = '""" + logInfo.name() + "';"
                    );
                    if(!staff_city_res.next())return false;
                    int staff_city_id = staff_city_res.getInt(1);

                    if (where_id != staff_city_id) return false;

                    statement.executeUpdate("""
                            update records
                            set item_state = 'From-Import Transporting'
                            where item_name = '""" + s + "'"
                    );
                    statement.executeUpdate("""
                            update records
                            set delivery_courier_id = """ + getid_people(logInfo.name()) + "\n" +
                            "where item_name = '" + s + "'"
                    );
                } else {
                    if (!itemState.equals(ItemState.Delivering) && !itemState.equals(ItemState.Finish)) return false;
                    if (!itemState.equals(ItemState.Finish)) {
                        statement.executeUpdate("""
                                update records
                                set item_state = 'Delivering'
                                where item_name = '""" + s + "'"
                        );
                    } else {
                        statement.executeUpdate("""
                                update records
                                set item_state = 'Finish'
                                where item_name = '""" + s + "'"
                        );
                    }
                }

            } else if (last_state.equals("Delivering")) {
                if (!itemState.equals(ItemState.Finish)) return false;

                ResultSet who_res = statement.executeQuery("""
                        select name
                        from records inner join staffs s
                        on records.delivery_courier_id = s.staff_id
                        where item_name = '""" + s + "'"
                );
                if(!who_res.next())return false;
                String who = who_res.getString(1);
                if (!who.equals(logInfo.name())) return false;
                statement.executeUpdate("""
                        update records
                        set item_state = 'Finish'
                        where item_name = '""" + s + "'"
                );
            }else{
                return false;
            }
            conn.close();
            return true;
        }
        catch(Exception e){
            return false;
        }
    }

    @Override
    public String[] getAllItemsAtPort (LogInfo logInfo){
        try {
            if (!userLogin(logInfo, LogInfo.StaffType.SeaportOfficer)) return new String[0];

            Class.forName("org.postgresql.Driver");
            Driver driver = new org.postgresql.Driver();
            Properties prop = new Properties();
            prop.setProperty("user", SOUser);
            prop.setProperty("password", SOPass);
            Connection conn = driver.connect("jdbc:postgresql://" + database, prop);
            Statement statement = conn.createStatement();
            ResultSet where_res = statement.executeQuery("""
					select city_id
					from staffs
					where name = '"""+logInfo.name()+"'"
            );
            if(!where_res.next())return new String[0];
            int cityId = where_res.getInt(1);
            ResultSet all_items = statement.executeQuery(
                    "select item_name\n" +
                            "from records\n" +
                            "where (export_city_id = "+cityId+") and (item_state = 'Export Checking')\n" +
                            "union all\n" +
                            "select item_name\n" +
                            "from records\n" +
                            "where (import_city_id ="+ cityId +") and (item_state = 'Import Checking')"
            );
            ArrayList<String> set = new ArrayList<>();
            int cnt = 0;
            while (all_items.next()) {
                set.add(all_items.getString(1));
                cnt++;
            }
            if(cnt == 0) return new String[0];
            String[] ret = new String[cnt];
            for (int i = 0; i < cnt; i++) {
                ret[i] = set.get(i);
            }
            conn.close();

            return ret;
        }
        catch (Exception e){
            return new String[0];
        }
    }

    @Override
    public boolean setItemCheckState(LogInfo logInfo, String s, boolean b){
        try {
            if (!userLogin(logInfo, LogInfo.StaffType.SeaportOfficer)) return false;
            Class.forName("org.postgresql.Driver");
            Driver driver = new org.postgresql.Driver();
            Properties prop = new Properties();
            prop.setProperty("user", SOUser);
            prop.setProperty("password", SOPass);
            Connection conn = driver.connect("jdbc:postgresql://" + database, prop);
            Statement statement = conn.createStatement();
            ResultSet id_res = statement.executeQuery("""
					select staff_id
					from staffs
					where name = '""" + logInfo.name()+"'"
            );
            if(!id_res.next())return false;
            int officerId = id_res.getInt(1);
            ResultSet exist = statement.executeQuery("""
                    select item_name, item_state
                    from records
                    where item_name = '""" + s + "'"
            );
            if(!exist.next())return false;
            String last_state = exist.getString(2);

            if (!last_state.equals("Export Checking") && !last_state.equals("Import Checking")) return false;
            if (last_state.equals("Export Checking")) {
                ResultSet who_res = statement.executeQuery("""
                        select name
                        from records left join staffs s
                        on records.export_officer_id = s.staff_id
                        where item_name = '""" + s + "'"
                );
                if(!who_res.next())return false;
                String who = who_res.getString(1);
                if (!(who == null) && !who.equals(logInfo.name())) return false;
                if (b) {
                    statement.executeUpdate("""
                            update records
                            set item_state = 'Packing to Container'
                            where item_name = '""" + s + "';\n"+
                            "update records\n" +
                            "set export_officer_id = "+officerId+"\n" +
                            "where item_name = '"+s+"'"
                    );
                } else {
                    statement.executeUpdate("""
                            update records
                            set item_state = 'Export Check Fail'
                            where item_name = '""" + s + "';\n"+
                            "update records\n" +
                            "set export_officer_id = "+officerId+"\n" +
                            "where item_name = '"+s+"'"
                    );
                }
            } else {
                ResultSet who_res = statement.executeQuery("""
                        select name
                        from records left join staffs s
                        on records.import_officer_id = s.staff_id
                        where item_name = '""" + s + "'"
                );
                if(!who_res.next())return false;
                String who = who_res.getString(1);
                if (!who.equals("") && !who.equals(logInfo.name())) return false;

                if (b) {
                    statement.executeUpdate("""
                            update records
                            set item_state = 'From-Import Transporting'
                            where item_name = '""" + s + "';\n"+
                            "update records\n" +
                            "set import_officer_id = "+officerId+"\n" +
                            "where item_name = '"+s+"'"
                    );
                } else {
                    statement.executeUpdate("""
                            update records
                            set item_state = 'Import Check Fail'
                            where item_name = '""" + s + "';\n"+
                            "update records\n" +
                            "set import_officer_id = "+officerId+"\n" +
                            "where item_name = '"+s+"'"
                    );
                }
            }

            return true;
        }
        catch (Exception e){
            return false;
        }

    }

    @Override
    public int getCompanyCount(LogInfo logInfo) {
        try{
            if(!userLogin(logInfo, LogInfo.StaffType.SustcManager)){
                return -1;
            }
            Class.forName("org.postgresql.Driver");
            Driver driver=new org.postgresql.Driver();
            Properties prop=new Properties();
            prop.setProperty("user", SDMUser);
            prop.setProperty("password", SDMPass);
            Connection conn=driver.connect("jdbc:postgresql://"+database,prop);
            Statement statement=conn.createStatement();
            ResultSet resultSet= statement.executeQuery("""
                    select count(*) as cnt
                    from companies""");
            if(!resultSet.next()){
                conn.close();
                return -1;
            }
            conn.close();
            return resultSet.getInt("cnt");
        }
        catch (Exception e){
            return -1;
        }
    }

    @Override
    public int getCityCount(LogInfo logInfo) {
        try{
            if(!userLogin(logInfo, LogInfo.StaffType.SustcManager)){
                return -1;
            }
            Class.forName("org.postgresql.Driver");
            Driver driver=new org.postgresql.Driver();
            Properties prop=new Properties();
            prop.setProperty("user", SDMUser);
            prop.setProperty("password", SDMPass);
            Connection conn=driver.connect("jdbc:postgresql://"+database,prop);
            Statement statement=conn.createStatement();
            ResultSet resultSet= statement.executeQuery("""
                    select count(*) as cnt
                    from cities""");
            if(!resultSet.next()){
                conn.close();
                return -1;
            }
            conn.close();
            return resultSet.getInt("cnt");
        }
        catch (Exception e){
            return -1;
        }
    }

    @Override
    public int getCourierCount(LogInfo logInfo) {
        try{
            if(!userLogin(logInfo, LogInfo.StaffType.SustcManager)){
                return -1;
            }
            Class.forName("org.postgresql.Driver");
            Driver driver=new org.postgresql.Driver();
            Properties prop=new Properties();
            prop.setProperty("user", SDMUser);
            prop.setProperty("password", SDMPass);
            Connection conn=driver.connect("jdbc:postgresql://"+database,prop);
            Statement statement=conn.createStatement();
            ResultSet resultSet= statement.executeQuery("""
                    select count(*) as cnt
                    from staffs
                    where staff_type='Courier'""");
            if(!resultSet.next()){
                conn.close();
                return -1;
            }
            conn.close();
            return resultSet.getInt("cnt");
        }
        catch (Exception e){
            return -1;
        }
    }

    @Override
    public int getShipCount(LogInfo logInfo) {
        try{
            if(!userLogin(logInfo, LogInfo.StaffType.SustcManager)){
                return -1;
            }
            Class.forName("org.postgresql.Driver");
            Driver driver=new org.postgresql.Driver();
            Properties prop=new Properties();
            prop.setProperty("user", SDMUser);
            prop.setProperty("password", SDMPass);
            Connection conn=driver.connect("jdbc:postgresql://"+database,prop);
            Statement statement=conn.createStatement();
            ResultSet resultSet= statement.executeQuery("""
                    select count(*) as cnt
                    from ships""");
            if(!resultSet.next()){
                conn.close();
                return -1;
            }
            conn.close();
            return resultSet.getInt("cnt");
        }
        catch (Exception e){
            return -1;
        }
    }

    public ItemState getItemState(String s){
        return switch (s){
            case "Picking-up" -> ItemState.PickingUp;
            case "To-Export Transporting" -> ItemState.ToExportTransporting;
            case "Export Checking" -> ItemState.ExportChecking;
            case "Export Check Fail" -> ItemState.ExportCheckFailed;
            case "Packing to Container" -> ItemState.PackingToContainer;
            case "Waiting for Shipping" -> ItemState.WaitingForShipping;
            case "Shipping" -> ItemState.Shipping;
            case "Unpacking from Container" -> ItemState.UnpackingFromContainer;
            case "Import Checking" -> ItemState.ImportChecking;
            case "Import Check Fail" -> ItemState.ImportCheckFailed;
            case "From-Import Transporting" -> ItemState.FromImportTransporting;
            case "Delivering" -> ItemState.Delivering;
            default ->ItemState.Finish;
        };
    }

    @Override
    public ItemInfo getItemInfo(LogInfo logInfo, String name) {
        try{
            if(!userLogin(logInfo, LogInfo.StaffType.SustcManager)){
                return null;
            }
            Class.forName("org.postgresql.Driver");
            Driver driver=new org.postgresql.Driver();
            Properties prop=new Properties();
            prop.setProperty("user", SDMUser);
            prop.setProperty("password", SDMPass);
            Connection conn=driver.connect("jdbc:postgresql://"+database,prop);
            Statement statement=conn.createStatement();

            ResultSet resultSet= statement.executeQuery("""
                    select item_name,item_class,item_price,item_state,
                           c4.city_name as retrieval_city,s2.name as retrieval_name,
                           c3.city_name as delivery_city,s.name as delivery_name,
                           c.city_name as import_city,s4.name as import_name,import_tax,
                           c2.city_name as export_city,s3.name as export_name,export_tax
                    from records
                    left join cities c on c.city_id = records.import_city_id
                    left join cities c2 on c2.city_id = records.export_city_id
                    left join staffs s on records.delivery_courier_id = s.staff_id
                    left join staffs s2 on records.retrieval_courier_id = s2.staff_id
                    left join cities c3 on c3.city_id = records.delivery_city_id
                    left join cities c4 on c4.city_id = records.retrieval_city_id
                    left join staffs s3 on s3.staff_id = records.export_officer_id
                    left join staffs s4 on s4.staff_id = records.import_officer_id
                    left join item_classes ic on ic.item_class_id = records.class_id
                    where item_name = '"""+name+"'");
            if(!resultSet.next()){
                return null;
            }
            return new ItemInfo(name,resultSet.getString("item_class"),resultSet.getDouble("item_price"),getItemState(resultSet.getString("item_state")),
                    new ItemInfo.RetrievalDeliveryInfo(resultSet.getString("retrieval_city"),resultSet.getString("retrieval_name")),
                    new ItemInfo.RetrievalDeliveryInfo(resultSet.getString("delivery_city"),resultSet.getString("delivery_name")),
                    new ItemInfo.ImportExportInfo(resultSet.getString("import_city"),resultSet.getString("import_name"),resultSet.getDouble("import_tax")),
                    new ItemInfo.ImportExportInfo(resultSet.getString("export_city"),resultSet.getString("export_name"),resultSet.getDouble("export_tax")));
        }
        catch (Exception e){
            return null;
        }
    }

    @Override
    public ShipInfo getShipInfo(LogInfo logInfo, String name) {
        try{
            if(!userLogin(logInfo, LogInfo.StaffType.SustcManager)){
                return null;
            }
            Class.forName("org.postgresql.Driver");
            Driver driver=new org.postgresql.Driver();
            Properties prop=new Properties();
            prop.setProperty("user", SDMUser);
            prop.setProperty("password", SDMPass);
            Connection conn=driver.connect("jdbc:postgresql://"+database,prop);
            Statement statement=conn.createStatement();
            ResultSet resultSet= statement.executeQuery("""
                    select *
                    from ships
                    left join companies c on ships.company_id = c.company_id
                    where ship_name='"""+name+"'");
            if(!resultSet.next()){
                return null;
            }
            String companyName=resultSet.getString("company_name");
            resultSet=statement.executeQuery("""
                    select *
                    from records
                    left join ships s on records.ship_id = s.ship_id
                    where ship_name='"""+name+"' and item_state='Shipping'");
            return new ShipInfo(name,companyName,resultSet.next());
        }
        catch (Exception e){
            return null;
        }
    }

    public ContainerInfo.Type getContainerType(String s){
        return switch (s) {
            case "Dry" -> ContainerInfo.Type.Dry;
            case "FlatRack" -> ContainerInfo.Type.FlatRack;
            case "ISOTank" -> ContainerInfo.Type.ISOTank;
            case "OpenTop" -> ContainerInfo.Type.OpenTop;
            default -> ContainerInfo.Type.Reefer;
        };
    }

    @Override
    public ContainerInfo getContainerInfo(LogInfo logInfo, String code) {
        try{
            if(!userLogin(logInfo, LogInfo.StaffType.SustcManager)){
                return null;
            }
            Class.forName("org.postgresql.Driver");
            Driver driver=new org.postgresql.Driver();
            Properties prop=new Properties();
            prop.setProperty("user", SDMUser);
            prop.setProperty("password", SDMPass);
            Connection conn=driver.connect("jdbc:postgresql://"+database,prop);
            Statement statement=conn.createStatement();
            ResultSet resultSet=statement.executeQuery("""
                    select *
                    from containers
                    where container_code='"""+code+"'");
            if(!resultSet.next()){
                return null;
            }
            ContainerInfo.Type type=getContainerType(resultSet.getString("container_type"));
            resultSet=statement.executeQuery("""
                    select *
                    from records
                    where container_code='"""+code+"'");
            return new ContainerInfo(type,code,resultSet.next());
        }
        catch (Exception e){
            return null;
        }
    }

    @Override
    public StaffInfo getStaffInfo(LogInfo logInfo, String name) {
        try{
            if(!userLogin(logInfo, LogInfo.StaffType.SustcManager)){
                return null;
            }
            Class.forName("org.postgresql.Driver");
            Driver driver=new org.postgresql.Driver();
            Properties prop=new Properties();
            prop.setProperty("user", SDMUser);
            prop.setProperty("password", SDMPass);
            Connection conn=driver.connect("jdbc:postgresql://"+database,prop);
            Statement statement=conn.createStatement();
            ResultSet resultSet= statement.executeQuery("""
                    select *
                    from staffs
                    left join companies com on com.company_id=staffs.company_id
                    left join cities c on staffs.city_id = c.city_id
                    where name='"""+name+"'");
            if(!resultSet.next()){
                return null;
            }
            return new StaffInfo(new LogInfo(name,get_stafftype(resultSet.getString("staff_type")),resultSet.getString("password")),resultSet.getString("company_name"),resultSet.getString("city_name"),resultSet.getString("gender").equals("female"),resultSet.getInt("age"),resultSet.getString("phone"));
        }
        catch (Exception e){
            return null;
        }
    }

}