import com.github.javafaker.Faker;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;

import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class DataGeneratorSQL
{

    private static final String JDBC_DRIVER = "org.mariadb.jdbc.Driver";
    private static final String DB_URL = "jdbc:mariadb://127.0.0.1/";

    //  Database credentials
    private static final String USERNAME = "root";
    private static final String PASSWORD = "root";

    private int asiakaskerroin = 0;
    private int laskukerroin = 0;
    private int tyokohdekerroin = 0;
    private int suorituskerroin = 0;

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


    public void insertData(int rowCount, int asiakaskerroin, int laskukerroin, int tyokohdekerroin, int suorituskerroin) {

        this.asiakaskerroin = asiakaskerroin;
        this.laskukerroin = laskukerroin;
        this.tyokohdekerroin = tyokohdekerroin;
        this.suorituskerroin = suorituskerroin;


        try {

            //org.neo4j.driver.v1.Driver driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "admin"));

            //Session session = driver.session();

            truncateDatabase();

//            session.run("MATCH (n) DETACH DELETE n");

            executeSQLInsert("INSERT INTO varasto.varastotarvike (id, nimi, varastosaldo, yksikko, sisaanostohinta, alv, poistettu) VALUES (0, 'MMJ 3X2,5MM² KAAPELI', 100, 'm', 0.64, 24, false)");
            executeSQLInsert("INSERT INTO varasto.varastotarvike (id, nimi, varastosaldo, yksikko, sisaanostohinta, alv, poistettu) VALUES (1, 'PISTORASIA 2-MAA OL JUSSI', 20, 'kpl', 17.90, 24, false)");
            executeSQLInsert("INSERT INTO varasto.varastotarvike (id, nimi, varastosaldo, yksikko, sisaanostohinta, alv, poistettu) VALUES (2, 'PISTORASIA KULMAMALLI 3-OSAINEN', 10, 'kpl', 14.90, 24, false)");
            executeSQLInsert("INSERT INTO varasto.varastotarvike (id, nimi, varastosaldo, yksikko, sisaanostohinta, alv, poistettu) VALUES (3, 'PEITELEVY 2-OS JUSSI', 20, 'kpl', 3.90, 24, false)");
            executeSQLInsert("INSERT INTO varasto.varastotarvike (id, nimi, varastosaldo, yksikko, sisaanostohinta, alv, poistettu) VALUES (4, 'PEITELEVY 1-OS JUSSI', 20, 'kpl', 2.90, 24, false)");
            executeSQLInsert("INSERT INTO varasto.varastotarvike (id, nimi, varastosaldo, yksikko, sisaanostohinta, alv, poistettu) VALUES (5, 'KYTKIN 5-SRJ UPPO JUSSI', 25, 'kpl', 11.90, 24, false)");
            executeSQLInsert("INSERT INTO varasto.varastotarvike (id, nimi, varastosaldo, yksikko, sisaanostohinta, alv, poistettu) VALUES (6, 'KYTKIN PINTA JUSSI 1/6', 10, 'kpl', 8.90, 24, false)");
            executeSQLInsert("INSERT INTO varasto.varastotarvike (id, nimi, varastosaldo, yksikko, sisaanostohinta, alv, poistettu) VALUES (7, 'PINNALLINEN RVP 5-KYTKIN', 5, 'kpl', 3.90, 24, false)");
            executeSQLInsert("INSERT INTO varasto.varastotarvike (id, nimi, varastosaldo, yksikko, sisaanostohinta, alv, poistettu) VALUES (8, 'SIDONTASPIRAALI 7,5-60MM LÄPINÄKYVÄ', 100, 'm', 0.09, 24, false)");

            /*
            session.run("CREATE (v:varastotarvike {varastotarvikeId: 0, nimi:\"MMJ 3X2,5MM² KAAPELI\", varastosaldo:\"100\", yksikko:\"m\", sisaanostohinta:\"0.64\", alv:\"24\", poistettu:\"false\"})");
            session.run("CREATE (v:varastotarvike {varastotarvikeid: 1, nimi:\"PISTORASIA 2-MAA OL JUSSI\", varastosaldo:\"20\", yksikko:\"kpl\", sisaanostohinta:\"17.90\", alv:\"24\", poistettu:\"false\"})");
            session.run("CREATE (v:varastotarvike {varastotarvikeid: 2, nimi:\"PISTORASIA KULMAMALLI 3-OSAINEN\", varastosaldo:\"10\", yksikko:\"kpl\", sisaanostohinta:\"14.90\", alv:\"24\", poistettu:\"false\"})");
            session.run("CREATE (v:varastotarvike {varastotarvikeid: 3, nimi:\"PEITELEVY 2-OS JUSSI\", varastosaldo:\"20\", yksikko:\"kpl\", sisaanostohinta:\"3.90\", alv:\"24\", poistettu:\"false\"})");
            session.run("CREATE (v:varastotarvike {varastotarvikeid: 4, nimi:\"PEITELEVY 1-OS JUSSI\", varastosaldo:\"20\", yksikko:\"kpl\", sisaanostohinta:\"2.90\", alv:\"24\", poistettu:\"false\"})");
            session.run("CREATE (v:varastotarvike {varastotarvikeid: 5, nimi:\"KYTKIN 5-SRJ UPPO JUSSI\", varastosaldo:\"25\", yksikko:\"kpl\", sisaanostohinta:\"11.90\", alv:\"24\", poistettu:\"false\"})");
            session.run("CREATE (v:varastotarvike {varastotarvikeid: 6, nimi:\"KYTKIN PINTA JUSSI 1/6\", varastosaldo:\"10\", yksikko:\"kpl\", sisaanostohinta:\"8.90\", alv:\"24\", poistettu:\"false\"})");
            session.run("CREATE (v:varastotarvike {varastotarvikeid: 7, nimi:\"PINNALLINEN RVP 5-KYTKIN\", varastosaldo:\"5\", yksikko:\"kpl\", sisaanostohinta:\"3.90\", alv:\"24\", poistettu:\"false\"})");
            session.run("CREATE (v:varastotarvike {varastotarvikeid: 8, nimi:\"SIDONTASPIRAALI 7,5-60MM LÄPINÄKYVÄ\", varastosaldo:\"100\", yksikko:\"m\", sisaanostohinta:\"0.09\", alv:\"24\", poistettu:\"false\"})");
            */

            executeSQLInsert("INSERT INTO varasto.tyotyyppi (id, nimi, hinta) VALUES (0, 'suunnittelu', 55)");
            executeSQLInsert("INSERT INTO varasto.tyotyyppi (id, nimi, hinta) VALUES (1, 'työ', 45)");
            executeSQLInsert("INSERT INTO varasto.tyotyyppi (id, nimi, hinta) VALUES (2, 'aputyö', 35)");

            /*
            session.run("CREATE (tt:tyotyyppi {tyotyyppiId: 0, nimi:\"suunnittelu\", hinta:\"55\"})");
            session.run("CREATE (tt:tyotyyppi {tyotyyppiid: 1, nimi:\"työ\", hinta:\"46\"})");
            session.run("CREATE (tt:tyotyyppi {tyotyyppiid: 2, nimi:\"aputyö\", hinta:\"35\"})");
            */


            for(int i=0; i < rowCount; i++) {
                insertRow(i);
            }

            //session.close();
            //driver.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    int asiakasindex = 0;
    int laskuindex = 0;
    int tyokohdeindex = 0;
    int suoritusindex = 0;
    int k = 0;

    public void insertRow(int index) {

        Faker faker = new Faker();

        System.out.println();
        System.out.println();


        int i = 0;

        int asiakasindexoriginal=asiakasindex;

        while(i < asiakaskerroin) {

            String name = faker.name().fullName(); // Miss Samanta Schmidt
            String streetAddress = faker.address().streetAddress(); // 60018 Sawayn Brooks Suite 449

            String sqlInsert = "INSERT INTO varasto.asiakas (id, nimi, osoite) VALUES (" + asiakasindex + ",\"" + name + "\",\"" + streetAddress + "\")";
            System.out.println(sqlInsert);
            executeSQLInsert(sqlInsert);
            i++;
            asiakasindex++;
        }

        asiakasindex=asiakasindexoriginal;

        /*
        String cypherCreate = "CREATE (a:asiakas {asiakasId: \"" + index + "\", nimi:\"" + name + "\",osoite:\"" + streetAddress + "\"})";
        System.out.println(cypherCreate);
        session.run(cypherCreate);
         */

        int laskuindexoriginal=laskuindex;

        i=0;
        int j = 0;
        while(i < asiakaskerroin) {
            j=0;
            while(j < laskukerroin) {

                //-- 0 = keskeneräinen, 1 = valmis, 2 = lähetetty, 3 = maksettu
                int tila = faker.random().nextInt(1, 3);
                java.util.Date dueDate = faker.date().past(360, TimeUnit.DAYS);
                DateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy");
                String dueDateAsString = dateFormat.format(dueDate);

                String sqlInsert = "INSERT INTO varasto.lasku (id, asiakasId, tila, erapaiva, ykkososa, edellinenlasku) VALUES (" + laskuindex +"," + asiakasindex + "," + tila + ",STR_TO_DATE('" + dueDateAsString + "','%d-%m-%Y'),0,0)";
                System.out.println(sqlInsert);
                executeSQLInsert(sqlInsert);
                laskuindex++;
                j++;
            }

            asiakasindex++;
            i++;
        }

        asiakasindex=asiakasindexoriginal;



        /*
        LocalDate localDate = dueDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
        int year  = localDate.getYear();
        int month = localDate.getMonthValue();
        int day   = localDate.getDayOfMonth();


        cypherCreate = "CREATE (l:lasku {laskuId: " + index + ", tila: " + tila + ", erapaiva: \"date({ year:" + year + ", month:" + month + ", day:" + day + " })\",ykkososa: 0, edellinenlasku: 0})";
        System.out.println(cypherCreate);
        session.run(cypherCreate);

        cypherCreate = "MATCH (a:asiakas),(l:lasku) WHERE a.asiakasId = \"" + index + "\" AND l.laskuId = " + index + " CREATE (a)-[m:MAKSAA]->(l)";
        System.out.println(cypherCreate);
        session.run(cypherCreate);
        */

        int tyokohdeindexoriginal=tyokohdeindex;

        i=0;
        while (i < asiakaskerroin) {
            j=0;
            while(j < tyokohdekerroin) {

                String name = faker.name().name();
                String streetAddress = faker.address().streetAddress();

                String sqlInsert = "INSERT INTO varasto.tyokohde (id, nimi, osoite, asiakasid) VALUES (" + tyokohdeindex +  ",\"" + name + "\",\"" + streetAddress + "\"," + asiakasindex + ")";
                System.out.println(sqlInsert);
                executeSQLInsert(sqlInsert);
                tyokohdeindex++;
                j++;
            }
            asiakasindex++;
            i++;
        }

        /*
        cypherCreate = "CREATE (t:tyokohde {tyokohdeId: " + index + ", nimi: \"" + name + "\", osoite: \"" + streetAddress + "\", asiakasid: " + index + " })";
        System.out.println(cypherCreate);
        session.run(cypherCreate);

        cypherCreate = "MATCH (a:asiakas),(t:tyokohde) WHERE a.asiakasId = \"" + index + "\" AND t.asiakasid = " + index + " CREATE (a)-[m:ASIAKKAAN_TYOKOHDE]->(t)";
        System.out.println(cypherCreate);
        session.run(cypherCreate);
        */

        int suoritusindexoriginal=suoritusindex;

        i=0;
        while(i < suorituskerroin) {

            int urakkahinta = faker.random().nextInt(1, 1000);
            //-- 0 = keskeneräinen, 1 = valmis, 2 = lähetetty, 3 = maksettu
            int tyyppi = faker.random().nextInt(1, 100);

            int randomlasku = random.nextInt(laskuindex);
            int randomtyokohde = random.nextInt(tyokohdeindex);

            String sqlInsert = "INSERT INTO varasto.suoritus (id, tyyppi, urakkahinta, laskuId, kohdeId) VALUES (" + suoritusindex + "," + tyyppi + "," + urakkahinta + "," + randomlasku + "," + randomtyokohde + ")";
            System.out.println(sqlInsert);
            executeSQLInsert(sqlInsert);

            k++;
            i++;
            suoritusindex++;
        }

        suoritusindex=suoritusindexoriginal;

        /*
        cypherCreate = "CREATE (s:suoritus {suoritusId: " + index + ", tyyppi: " + tyyppi + ", urakkahinta: " + urakkahinta + ", laskuId: " + index + ", tyokohdeId: " + index + "})";
        System.out.println(cypherCreate);
        session.run(cypherCreate);

        cypherCreate = "MATCH (s:suoritus),(l:lasku) WHERE s.laskuId = " + index + " AND l.laskuId = " + index + " CREATE (s)-[m:SUORITUKSEN_LASKU]->(l)";
        System.out.println(cypherCreate);
        session.run(cypherCreate);

        cypherCreate = "MATCH (s:suoritus),(t:tyokohde) WHERE s.tyokohdeId = " + index + " AND t.tyokohdeId = " + index + " CREATE (s)-[m:SUORITUKSEN_TYOKOHDE]->(t)";
        System.out.println(cypherCreate);
        session.run(cypherCreate);
        */

        int alennusprosentti = faker.random().nextInt(1, 100);
        double alennus = (0.01 * alennusprosentti);

        i=0;
        while(i < suorituskerroin) {

            int lukumaara = faker.random().nextInt(1, 100);

            int varastotarvikeId = faker.random().nextInt(0, 8);

            String sqlInsert = "INSERT INTO varasto.kaytettytarvike (lukumaara, alennus, suoritusId, varastotarvikeId) VALUES(" + lukumaara + "," + alennus + "," + suoritusindex + "," + varastotarvikeId + ")";
            System.out.println(sqlInsert);
            executeSQLInsert(sqlInsert);
            i++;
            suoritusindex++;
        }

        suoritusindex=suoritusindexoriginal;

        /*
        cypherCreate = "MATCH (s:suoritus),(v:varastotarvike) WHERE s.suoritusId=" + index + " AND v.varastotarvikeId=" + varastotarvikeId +
                " CREATE (s)-[m:KAYTETTY_TARVIKE {lukumaara:" + lukumaara + ", alennus:" + alennus + "}]->(v)" ;
        System.out.println(cypherCreate);
        session.run(cypherCreate);
        */

        i=0;
        while(i < suorituskerroin) {

            int tuntimaara = faker.random().nextInt(1, 100);
            int tyotyyppiId = faker.random().nextInt(0, 2);

            String sqlInsert = "INSERT INTO varasto.tyotunnit (tyotyyppiId, tuntimaara, alennus, suoritusId) VALUES(" + tyotyyppiId + "," + tuntimaara + "," + alennus + "," + suoritusindex + ")";
            System.out.println(sqlInsert);
            executeSQLInsert(sqlInsert);
            i++;
            suoritusindex++;
        }




        /*
        cypherCreate = "MATCH (s:suoritus),(tt:tyotyyppi) WHERE s.suoritusId=" + index + " AND tt.tyotyyppiId=" + tyotyyppiId +
                " CREATE (s)-[m:TYOTUNNIT {tuntimaara:" + tuntimaara + ", alennus:" + alennus + "}]->(tt)";
        System.out.println(cypherCreate);
        session.run(cypherCreate);
        */

    }

}