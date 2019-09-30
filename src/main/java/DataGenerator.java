import com.github.javafaker.Faker;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;

import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class DataGenerator
{

    private static final String JDBC_DRIVER = "org.mariadb.jdbc.Driver";
    private static final String DB_URL = "jdbc:mariadb://127.0.0.1/";

    //  Database credentials
    private static final String USERNAME = "root";
    private static final String PASSWORD = "root";

    private int customerFactor = 0;
    private int invoiceFactor = 0;
    private int targetFactor = 0;
    private int workFactor = 0;
    private int itemFactor = 0;
    private int sequentialInvoices = 0;


    private List<String> firstnames;
    private List<String> surnames;
    private List<HashMap<String, String>> addresses;

    private Random random = new Random();

    public ResultSet executeSQLInsert(String sqlQuery) {

        Connection conn = null;
        Statement stmt = null;
        ResultSet resultSet = null;

        try {

            Class.forName(JDBC_DRIVER);

            conn = DriverManager.getConnection(DB_URL, USERNAME, PASSWORD);
            stmt = conn.createStatement();

            resultSet = stmt.executeQuery(sqlQuery);


        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {

            try {
                if (stmt != null) {
                    conn.close();
                }
            } catch (SQLException se) {
            }
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException se) {
                se.printStackTrace();
            }
        }

        return resultSet;
    }


    public ResultSet executeSQLQuery(String sqlQuery) {

        Connection conn = null;
        Statement stmt = null;
        ResultSet resultSet = null;

        try {

            Class.forName(JDBC_DRIVER);

            conn = DriverManager.getConnection(DB_URL, USERNAME, PASSWORD);
            stmt = conn.createStatement();

            resultSet = stmt.executeQuery(sqlQuery);

        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {

            try {
                if (stmt != null) {
                    conn.close();
                }
            } catch (SQLException se) {
            }
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException se) {
                se.printStackTrace();
            }
        }

        return resultSet;
    }

    public ResultSet truncateDatabase() {

        Connection conn = null;
        Statement stmt = null;
        ResultSet resultSet = null;

        try {

            Class.forName(JDBC_DRIVER);

            conn = DriverManager.getConnection(DB_URL, USERNAME, PASSWORD);
            stmt = conn.createStatement();

            stmt.addBatch("SET FOREIGN_KEY_CHECKS=0;");
            stmt.addBatch("TRUNCATE TABLE varasto.asiakas;");
            stmt.addBatch("TRUNCATE TABLE varasto.lasku;");
            stmt.addBatch("TRUNCATE TABLE varasto.tyokohde;");
            stmt.addBatch("TRUNCATE TABLE varasto.suoritus;");
            stmt.addBatch("TRUNCATE TABLE varasto.varastotarvike;");
            stmt.addBatch("TRUNCATE TABLE varasto.kaytettytarvike;");
            stmt.addBatch("TRUNCATE TABLE varasto.tyotyyppi;");
            stmt.addBatch("TRUNCATE TABLE varasto.tyotunnit;");
            stmt.addBatch("SET FOREIGN_KEY_CHECKS=1;");
            stmt.executeBatch();

        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {

            try {
                if (stmt != null) {
                    conn.close();
                }
            } catch (SQLException se) {
            }
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException se) {
                se.printStackTrace();
            }
        }

        return resultSet;
    }

    public void getSampleData() {

        try {

            firstnames = new ArrayList<String>();
            surnames = new ArrayList<String>();
            addresses = new ArrayList<HashMap<String, String>>();

            ResultSet rs = executeSQLQuery("SELECT name FROM testdata.firstnames;");
            while (rs.next()) {
                String firstName = rs.getString("name");
                firstnames.add(firstName);
            }

            rs = executeSQLQuery("SELECT name FROM testdata.surnames;");
            while (rs.next()) {
                String surName = rs.getString("name");
                surnames.add(surName);
            }

            rs = executeSQLQuery("SELECT street, city, district, region, postcode FROM testdata.addresses;");
            while (rs.next()) {

                HashMap<String, String> address = new HashMap<String, String>();

                address.put("street", rs.getString("street"));
                address.put("city", rs.getString("city"));
                address.put("district", rs.getString("district"));
                address.put("region", rs.getString("region"));
                address.put("postcode", rs.getString("postcode"));

                addresses.add(address);
            }


        } catch (Exception e) {
            System.err.println("Exception: "
                    +e.getMessage());
        }

    }

    enum TyoTyyppi {
        työ, suunnittelu, aputyö
    }

    class RandomEnum<E extends Enum<TyoTyyppi>> {
        Random r = new Random();
        E[] values;

        public RandomEnum(Class<E> token) {
            values = token.getEnumConstants();
        }

        public E random() {
            return values[r.nextInt(values.length)];
        }
    }


    public void insertData(int rowCount, int customerFactor, int invoiceFactor, int sequentialInvoices, int targetFactor, int workFactor, int itemFactor) {

        this.customerFactor = customerFactor;
        this.invoiceFactor = invoiceFactor;
        this.sequentialInvoices = sequentialInvoices;
        this.targetFactor = targetFactor;
        this.workFactor = workFactor;
        this.itemFactor = itemFactor;

        try {

            org.neo4j.driver.v1.Driver driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "admin"));

            Session session = driver.session();

            truncateDatabase();

            session.run("MATCH (n) DETACH DELETE n");

            executeSQLInsert("INSERT INTO varasto.varastotarvike (id, nimi, varastosaldo, yksikko, sisaanostohinta, alv, poistettu) VALUES (0, 'MMJ 3X2,5MM² KAAPELI', 100, 'm', 0.64, 24, false)");
            executeSQLInsert("INSERT INTO varasto.varastotarvike (id, nimi, varastosaldo, yksikko, sisaanostohinta, alv, poistettu) VALUES (1, 'PISTORASIA 2-MAA OL JUSSI', 20, 'kpl', 17.90, 24, false)");
            executeSQLInsert("INSERT INTO varasto.varastotarvike (id, nimi, varastosaldo, yksikko, sisaanostohinta, alv, poistettu) VALUES (2, 'PISTORASIA KULMAMALLI 3-OSAINEN', 10, 'kpl', 14.90, 24, false)");
            executeSQLInsert("INSERT INTO varasto.varastotarvike (id, nimi, varastosaldo, yksikko, sisaanostohinta, alv, poistettu) VALUES (3, 'PEITELEVY 2-OS JUSSI', 20, 'kpl', 3.90, 24, false)");
            executeSQLInsert("INSERT INTO varasto.varastotarvike (id, nimi, varastosaldo, yksikko, sisaanostohinta, alv, poistettu) VALUES (4, 'PEITELEVY 1-OS JUSSI', 20, 'kpl', 2.90, 24, false)");
            executeSQLInsert("INSERT INTO varasto.varastotarvike (id, nimi, varastosaldo, yksikko, sisaanostohinta, alv, poistettu) VALUES (5, 'KYTKIN 5-SRJ UPPO JUSSI', 25, 'kpl', 11.90, 24, false)");
            executeSQLInsert("INSERT INTO varasto.varastotarvike (id, nimi, varastosaldo, yksikko, sisaanostohinta, alv, poistettu) VALUES (6, 'KYTKIN PINTA JUSSI 1/6', 10, 'kpl', 8.90, 24, false)");
            executeSQLInsert("INSERT INTO varasto.varastotarvike (id, nimi, varastosaldo, yksikko, sisaanostohinta, alv, poistettu) VALUES (7, 'PINNALLINEN RVP 5-KYTKIN', 5, 'kpl', 3.90, 24, false)");
            executeSQLInsert("INSERT INTO varasto.varastotarvike (id, nimi, varastosaldo, yksikko, sisaanostohinta, alv, poistettu) VALUES (8, 'SIDONTASPIRAALI 7,5-60MM LÄPINÄKYVÄ', 100, 'm', 0.09, 24, false)");


            session.run("CREATE (v:varastotarvike {varastotarvikeId: 0, nimi:\"MMJ 3X2,5MM² KAAPELI\", varastosaldo:\"100\", yksikko:\"m\", sisaanostohinta:\"0.64\", alv:\"24\", poistettu:\"false\"})");
            session.run("CREATE (v:varastotarvike {varastotarvikeid: 1, nimi:\"PISTORASIA 2-MAA OL JUSSI\", varastosaldo:\"20\", yksikko:\"kpl\", sisaanostohinta:\"17.90\", alv:\"24\", poistettu:\"false\"})");
            session.run("CREATE (v:varastotarvike {varastotarvikeid: 2, nimi:\"PISTORASIA KULMAMALLI 3-OSAINEN\", varastosaldo:\"10\", yksikko:\"kpl\", sisaanostohinta:\"14.90\", alv:\"24\", poistettu:\"false\"})");
            session.run("CREATE (v:varastotarvike {varastotarvikeid: 3, nimi:\"PEITELEVY 2-OS JUSSI\", varastosaldo:\"20\", yksikko:\"kpl\", sisaanostohinta:\"3.90\", alv:\"24\", poistettu:\"false\"})");
            session.run("CREATE (v:varastotarvike {varastotarvikeid: 4, nimi:\"PEITELEVY 1-OS JUSSI\", varastosaldo:\"20\", yksikko:\"kpl\", sisaanostohinta:\"2.90\", alv:\"24\", poistettu:\"false\"})");
            session.run("CREATE (v:varastotarvike {varastotarvikeid: 5, nimi:\"KYTKIN 5-SRJ UPPO JUSSI\", varastosaldo:\"25\", yksikko:\"kpl\", sisaanostohinta:\"11.90\", alv:\"24\", poistettu:\"false\"})");
            session.run("CREATE (v:varastotarvike {varastotarvikeid: 6, nimi:\"KYTKIN PINTA JUSSI 1/6\", varastosaldo:\"10\", yksikko:\"kpl\", sisaanostohinta:\"8.90\", alv:\"24\", poistettu:\"false\"})");
            session.run("CREATE (v:varastotarvike {varastotarvikeid: 7, nimi:\"PINNALLINEN RVP 5-KYTKIN\", varastosaldo:\"5\", yksikko:\"kpl\", sisaanostohinta:\"3.90\", alv:\"24\", poistettu:\"false\"})");
            session.run("CREATE (v:varastotarvike {varastotarvikeid: 8, nimi:\"SIDONTASPIRAALI 7,5-60MM LÄPINÄKYVÄ\", varastosaldo:\"100\", yksikko:\"m\", sisaanostohinta:\"0.09\", alv:\"24\", poistettu:\"false\"})");


            executeSQLInsert("INSERT INTO varasto.tyotyyppi (id, nimi, hinta) VALUES (0, 'suunnittelu', 55)");
            executeSQLInsert("INSERT INTO varasto.tyotyyppi (id, nimi, hinta) VALUES (1, 'työ', 45)");
            executeSQLInsert("INSERT INTO varasto.tyotyyppi (id, nimi, hinta) VALUES (2, 'aputyö', 35)");


            session.run("CREATE (tt:tyotyyppi {tyotyyppiId: 0, nimi:\"suunnittelu\", hinta:\"55\"})");
            session.run("CREATE (tt:tyotyyppi {tyotyyppiid: 1, nimi:\"työ\", hinta:\"46\"})");
            session.run("CREATE (tt:tyotyyppi {tyotyyppiid: 2, nimi:\"aputyö\", hinta:\"35\"})");



            for(int i=0; i < rowCount; i++) {
                insertRow(i, session);
            }

            session.close();
            driver.close();


        } catch (Exception e) {
            e.printStackTrace();
        }
    }



    int customerIndex = 0;
    int invoiceIndex = 0;
    int targetIndex = 0;
    int workIndex = 0;

    int firstnameindex = 0;
    int surnameindex = 0;
    int addressindex = 0;


    public void resetIndexes() {

        if (firstnameindex >= firstnames.size()) {
            firstnameindex = 0;
        }

        if (surnameindex >= surnames.size()) {
            surnameindex = 0;
        }

        if (addressindex >= addresses.size()) {
            addressindex = 0;

        }

    }

    public void insertRow(int index, Session session) {

        Faker faker = new Faker();

        int i = 0;
        int j = 0;
        int k = 0;

        int customerIndexOriginal=customerIndex;

        while(i < customerFactor) {

            resetIndexes();

            String name = firstnames.get(firstnameindex) + " " + surnames.get(surnameindex);

            String streetAddress = addresses.get(addressindex).get("street") + " " + addresses.get(addressindex).get("city") + " " + addresses.get(addressindex).get("district") + " " + addresses.get(addressindex).get("region") + " " + addresses.get(addressindex).get("postcode");

            String sqlInsert = "INSERT INTO varasto.asiakas (id, nimi, osoite) VALUES (" + customerIndex + ",\"" + name + "\",\"" + streetAddress + "\")";
            executeSQLInsert(sqlInsert);

            String cypherCreate = "CREATE (a:asiakas {asiakasId: \"" + customerIndex + "\", nimi:\"" + name + "\",osoite:\"" + streetAddress + "\"})";
            session.run(cypherCreate);

            i++;
            customerIndex++;
            firstnameindex++;
            surnameindex++;
            addressindex++;

        }

        customerIndex=customerIndexOriginal;
        int invoiceIndexOriginal=invoiceIndex;

        String cypherCreate = null;

        i = 0;
        while (i < customerFactor) {
            int customerInvoiceIndexOriginal=invoiceIndex;
            j = 0;
            while (j < invoiceFactor) {

                //-- 0 = keskeneräinen, 1 = valmis, 2 = lähetetty, 3 = maksettu
                int tila = faker.random().nextInt(1, 3);
                java.util.Date dueDate = faker.date().past(360, TimeUnit.DAYS);
                DateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy");
                String dueDateAsString = dateFormat.format(dueDate);

                String sqlInsert = "";

                if(j < sequentialInvoices ) {

                    if(invoiceIndex == customerInvoiceIndexOriginal) {
                        sqlInsert = "INSERT INTO varasto.lasku (id, asiakasId, tila, erapaiva, ykkososa, edellinenlasku) VALUES (" + invoiceIndex + "," + customerIndex + "," + tila + ",STR_TO_DATE('" + dueDateAsString + "','%d-%m-%Y')," + customerInvoiceIndexOriginal + "," + (invoiceIndex) + ")";

                    }
                    else {
                        sqlInsert = "INSERT INTO varasto.lasku (id, asiakasId, tila, erapaiva, ykkososa, edellinenlasku) VALUES (" + invoiceIndex + "," + customerIndex + "," + tila + ",STR_TO_DATE('" + dueDateAsString + "','%d-%m-%Y')," + customerInvoiceIndexOriginal + "," + (invoiceIndex-1) + ")";

                    }

                } else {

                    sqlInsert = "INSERT INTO varasto.lasku (id, asiakasId, tila, erapaiva, ykkososa, edellinenlasku) VALUES (" + invoiceIndex + "," + customerIndex + "," + tila + ",STR_TO_DATE('" + dueDateAsString + "','%d-%m-%Y')," + invoiceIndex + "," + invoiceIndex + ")";

                }

                executeSQLInsert(sqlInsert);

                LocalDate localDate = dueDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
                int year = localDate.getYear();
                int month = localDate.getMonthValue();
                int day = localDate.getDayOfMonth();

                cypherCreate = "CREATE (l:lasku {laskuId: " + invoiceIndex + ", asiakasId: " + customerIndex + ", tila: " + tila + ", erapaiva: \"date({ year:" + year + ", month:" + month + ", day:" + day + " })\",ykkososa: 0, edellinenlasku: 0})";
                session.run(cypherCreate);

                cypherCreate = "MATCH (a:asiakas),(l:lasku) WHERE a.asiakasId = \"" + customerIndex + "\" AND l.laskuId = " + invoiceIndex + " CREATE (a)-[m:MAKSAA]->(l)";
                session.run(cypherCreate);

                invoiceIndex++;
                j++;
            }

            customerIndex++;
            i++;
        }



        customerIndex=customerIndexOriginal;
        int targetIndexOriginal=targetIndex;

        i=0;
        while (i < customerFactor) {
            j=0;
            while(j < targetFactor) {

                resetIndexes();

                String name = firstnames.get(firstnameindex) + " " + surnames.get(surnameindex);

                String streetAddress = addresses.get(addressindex).get("street") + " " + addresses.get(addressindex).get("city") + " " + addresses.get(addressindex).get("district") + " " + addresses.get(addressindex).get("region") + " " + addresses.get(addressindex).get("postcode");

                String sqlInsert = "INSERT INTO varasto.tyokohde (id, nimi, osoite, asiakasid) VALUES (" + targetIndex +  ",\"" + name + "\",\"" + streetAddress + "\"," + customerIndex + ")";
                executeSQLInsert(sqlInsert);

                cypherCreate = "CREATE (t:tyokohde {tyokohdeId: " + targetIndex + ", nimi: \"" + name + "\", osoite: \"" + streetAddress + "\", asiakasid: " + customerIndex + " })";
                session.run(cypherCreate);

                cypherCreate = "MATCH (a:asiakas),(t:tyokohde) WHERE a.asiakasId = \"" + customerIndex + "\" AND t.tyokohdeid = " + targetIndex + " CREATE (a)-[m:ASIAKKAAN_TYOKOHDE]->(t)";
                session.run(cypherCreate);

                targetIndex++;
                firstnameindex++;
                surnameindex++;
                addressindex++;
                j++;
            }


            customerIndex++;
            i++;
        }

        int workIndexOriginal=workIndex;
        targetIndex=targetIndexOriginal;
        invoiceIndex=invoiceIndexOriginal;

        i = 0;
        while(i < targetFactor*customerFactor) {

            j=0;
            while(j < workFactor) {

                invoiceIndex=invoiceIndexOriginal;

                k=0;
                while(k < invoiceFactor*customerFactor) {

                    int urakkahinta = faker.random().nextInt(1, 1000);
                    //-- 0 = keskeneräinen, 1 = valmis, 2 = lähetetty, 3 = maksettu
                    int tyyppi = faker.random().nextInt(1, 3);

                    String sqlInsert = "INSERT INTO varasto.suoritus (id, tyyppi, urakkahinta, laskuId, kohdeId) VALUES (" + workIndex + "," + tyyppi + "," + urakkahinta + "," + invoiceIndex + "," + targetIndex + ")";
                    executeSQLInsert(sqlInsert);

                    cypherCreate = "CREATE (s:suoritus {suoritusId: " + workIndex + ", tyyppi: " + tyyppi + ", urakkahinta: " + urakkahinta + ", laskuId: " + invoiceIndex + ", tyokohdeId: " + targetIndex + "})";
                    session.run(cypherCreate);

                    cypherCreate = "MATCH (s:suoritus),(l:lasku) WHERE s.suoritusId = " + workIndex + " AND l.laskuId = " + invoiceIndex + " CREATE (s)-[m:SUORITUKSEN_LASKU]->(l)";
                    session.run(cypherCreate);

                    cypherCreate = "MATCH (s:suoritus),(t:tyokohde) WHERE s.suoritusId = " + workIndex + "  AND t.tyokohdeId = " + targetIndex + " CREATE (s)-[m:SUORITUKSEN_TYOKOHDE]->(t)";
                    session.run(cypherCreate);

                    invoiceIndex++;
                    k++;

                }

                workIndex++;
                j++;
            }

            targetIndex++;
            i++;
        }

        workIndex=workIndexOriginal;

        int alennusprosentti = faker.random().nextInt(1, 100);
        double alennus = (0.01 * alennusprosentti);

        i=0;
        while(i < targetFactor*customerFactor*workFactor) {

            j = 0;
            while (j <= itemFactor) {

                int lukumaara = faker.random().nextInt(1, 100);

                int varastotarvikeId = j;

                String sqlInsert = "INSERT INTO varasto.kaytettytarvike (lukumaara, alennus, suoritusId, varastotarvikeId) VALUES(" + lukumaara + "," + alennus + "," + workIndex + "," + varastotarvikeId + ")";
                executeSQLInsert(sqlInsert);

                cypherCreate = "MATCH (s:suoritus),(v:varastotarvike) WHERE s.suoritusId=" + workIndex + " AND v.varastotarvikeId=" + varastotarvikeId +
                    " CREATE (s)-[m:KAYTETTY_TARVIKE {lukumaara:" + lukumaara + ", alennus:" + alennus + "}]->(v)";
                session.run(cypherCreate);

                j++;

            }

            i++;
            workIndex++;
        }

        workIndex=workIndexOriginal;

        i=0;
        while(i < targetFactor*customerFactor*workFactor) {

            j = 0;
            while (j < 3) {

                int tuntimaara = faker.random().nextInt(0, 100);
                int tyotyyppiId = j;

                String sqlInsert = "INSERT INTO varasto.tyotunnit (tyotyyppiId, tuntimaara, alennus, suoritusId) VALUES(" + tyotyyppiId + "," + tuntimaara + "," + alennus + "," + workIndex + ")";
                executeSQLInsert(sqlInsert);

                cypherCreate = "MATCH (s:suoritus),(tt:tyotyyppi) WHERE s.suoritusId=" + workIndex + " AND tt.tyotyyppiId=" + tyotyyppiId +
                        " CREATE (s)-[m:TYOTUNNIT {tuntimaara:" + tuntimaara + ", alennus:" + alennus + "}]->(tt)";
                session.run(cypherCreate);
                j++;

            }

            i++;
            workIndex++;
        }

    }

}