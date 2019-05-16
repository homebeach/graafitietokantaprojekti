import com.github.javafaker.Faker;
import graph.Graph;

import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class DataGenerator
{

    static final String JDBC_DRIVER = "org.mariadb.jdbc.Driver";
    static final String DB_URL = "jdbc:mariadb://127.0.0.1/";

    //  Database credentials
    static final String USERNAME = "root";
    static final String PASSWORD = "root";

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


    public void insertData(int rowCount) {

        executeSQLInsert("INSERT INTO varasto.varastotarvike (nimi, varastosaldo, yksikko, sisaanostohinta, alv, poistettu) VALUES ('MMJ 3X2,5MM² KAAPELI', 100, 'm', 0.64, 24, false)");
        executeSQLInsert("INSERT INTO varasto.varastotarvike (nimi, varastosaldo, yksikko, sisaanostohinta, alv, poistettu) VALUES ('PISTORASIA 2-MAA OL JUSSI', 20, 'kpl', 17.90, 24, false)");
        executeSQLInsert("INSERT INTO varasto.varastotarvike (nimi, varastosaldo, yksikko, sisaanostohinta, alv, poistettu) VALUES ('PISTORASIA KULMAMALLI 3-OSAINEN', 10, 'kpl', 14.90, 24, false)");
        executeSQLInsert("INSERT INTO varasto.varastotarvike (nimi, varastosaldo, yksikko, sisaanostohinta, alv, poistettu) VALUES ('PEITELEVY 2-OS JUSSI', 20, 'kpl', 3.90, 24, false)");
        executeSQLInsert("INSERT INTO varasto.varastotarvike (nimi, varastosaldo, yksikko, sisaanostohinta, alv, poistettu) VALUES ('PEITELEVY 1-OS JUSSI', 20, 'kpl', 2.90, 24, false)");
        executeSQLInsert("INSERT INTO varasto.varastotarvike (nimi, varastosaldo, yksikko, sisaanostohinta, alv, poistettu) VALUES ('KYTKIN 5-SRJ UPPO JUSSI', 25, 'kpl', 11.90, 24, false)");
        executeSQLInsert("INSERT INTO varasto.varastotarvike (nimi, varastosaldo, yksikko, sisaanostohinta, alv, poistettu) VALUES ('KYTKIN PINTA JUSSI 1/6', 10, 'kpl', 8.90, 24, false)");
        executeSQLInsert("INSERT INTO varasto.varastotarvike (nimi, varastosaldo, yksikko, sisaanostohinta, alv, poistettu) VALUES ('PINNALLINEN RVP 5-KYTKIN', 5, 'kpl', 3.90, 24, false)");
        executeSQLInsert("INSERT INTO varasto.varastotarvike (nimi, varastosaldo, yksikko, sisaanostohinta, alv, poistettu) VALUES ('SIDONTASPIRAALI 7,5-60MM LÄPINÄKYVÄ', 100, 'm', 0.09, 24, false)");

        executeSQLInsert("INSERT INTO varasto.tyotyyppi (nimi, hinta) VALUES ('suunnittelu', 55)");
        executeSQLInsert("INSERT INTO varasto.tyotyyppi (nimi, hinta) VALUES ('työ', 45)");
        executeSQLInsert("INSERT INTO varasto.tyotyyppi (nimi, hinta) VALUES ('aputyö', 35)");

        for(int i=0; i < rowCount; i++) {
            insertRow();
        }

    }



    public void insertRow()
    {

        Faker faker = new Faker();

        String name = faker.name().fullName(); // Miss Samanta Schmidt
        String streetAddress = faker.address().streetAddress(); // 60018 Sawayn Brooks Suite 449

        String sqlInsert = "INSERT INTO varasto.asiakas (nimi, osoite) VALUES (\"" + name + "\",\"" + streetAddress + "\")";

        System.out.println(sqlInsert);

        executeSQLInsert(sqlInsert);

        String sqlQuery = "SELECT id FROM varasto.asiakas WHERE nimi=\"" + name + "\" AND osoite=\"" + streetAddress + "\";";

        System.out.println(sqlQuery);

        ResultSet resultSet = executeSQLQuery(sqlQuery);

        int asiakasId = 0;

        try {

            while(resultSet.next()) {

                asiakasId = resultSet.getInt("id");

                System.out.println("asiakasId: " + asiakasId);

                break;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        java.util.Date dueDate = faker.date().past(360, TimeUnit.DAYS);

        DateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy");

        String dueDateAsString = dateFormat.format(dueDate);

        sqlInsert = "INSERT INTO varasto.lasku (asiakasId, tila, erapaiva, ykkososa, edellinenlasku) VALUES (" + asiakasId + ",1,STR_TO_DATE('" + dueDateAsString + "','%d-%m-%Y'),1,0)";

        System.out.println(sqlInsert);

        executeSQLInsert(sqlInsert);

        sqlQuery = "SELECT id FROM varasto.lasku WHERE asiakasId=" + asiakasId + " AND erapaiva=STR_TO_DATE('" + dueDateAsString + "','%d-%m-%Y');";

        System.out.println(sqlQuery);

        resultSet = executeSQLQuery(sqlQuery);

        int laskuId = 0;

        try {

            while(resultSet.next()) {

                laskuId = resultSet.getInt("id");

                System.out.println("laskuId: " + laskuId);

            break;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }


        name = faker.name().fullName();
        streetAddress = faker.address().streetAddress();

        sqlInsert = "INSERT INTO varasto.tyokohde (nimi, osoite, asiakasid) VALUES (\"" + name + "\",\"" + streetAddress + "\"," + asiakasId + ")";

        System.out.println(sqlInsert);

        executeSQLInsert(sqlInsert);

        sqlQuery = "SELECT id FROM varasto.tyokohde WHERE asiakasId=" + asiakasId + " AND nimi=\"" + name + "\" AND osoite='" + streetAddress + "';";

        System.out.println(sqlQuery);

        resultSet = executeSQLQuery(sqlQuery);

        int tyokohdeId = 0;

        try {

            while(resultSet.next()) {

                tyokohdeId = resultSet.getInt("id");

                System.out.println("tyokohdeId: " + tyokohdeId);

            break;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        int hinta = faker.random().nextInt(1,1000);

        sqlInsert = "INSERT INTO varasto.suoritus (tyyppi, urakkahinta, laskuId, kohdeId) VALUES (2," + hinta + "," + laskuId + "," + tyokohdeId + ")";

        System.out.println(sqlInsert);

        executeSQLInsert(sqlInsert);

        sqlQuery = "SELECT id FROM varasto.suoritus WHERE laskuId=" + laskuId + " AND kohdeId=" + tyokohdeId + ";";

        resultSet = executeSQLQuery(sqlQuery);

        int suoritusId = 0;

        try {

            while(resultSet.next()) {

                suoritusId = resultSet.getInt("id");

                System.out.println("suoritusId: " + suoritusId);

            break;
            }


        } catch (Exception e) {
            e.printStackTrace();
        }


        sqlQuery = "SELECT id FROM varasto.varastotarvike ORDER BY RAND() LIMIT 1";

        System.out.println(sqlQuery);

        resultSet = executeSQLQuery(sqlQuery);

        int varastoTarvikeId = 0;

        try {

            while(resultSet.next()) {

                varastoTarvikeId = resultSet.getInt("id");

                System.out.println("varastoTarvikeId: " + varastoTarvikeId);

            break;
            }


        } catch (Exception e) {
            e.printStackTrace();
        }

        int lukumaara = faker.random().nextInt(1,100);

        int alennusprosentti = faker.random().nextInt(1,100);

        double alennus = (0.01 * alennusprosentti);

        sqlInsert = "INSERT INTO varasto.kaytettytarvike (lukumaara, alennus, suoritusId, varastotarvikeId) VALUES(" + lukumaara + "," + alennus + "," + suoritusId + "," + varastoTarvikeId + ")";

        System.out.println(sqlInsert);

        executeSQLInsert(sqlInsert);

        sqlQuery = "SELECT id FROM varasto.tyotyyppi ORDER BY RAND() LIMIT 1";

        System.out.println(sqlQuery);

        resultSet = executeSQLQuery(sqlQuery);

        int tyotyyppiId = 0;

        try {

            while(resultSet.next()) {

                tyotyyppiId = resultSet.getInt("id");

                System.out.println("tyotyyppiId: " + tyotyyppiId);

            break;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        sqlInsert = "INSERT INTO varasto.tyotunnit (tyotyyppiId, tuntimaara, alennus, suoritusId) VALUES(" + tyotyyppiId + "," + lukumaara + "," + alennus + "," + suoritusId + ")";

        System.out.println(sqlInsert);

        executeSQLInsert(sqlInsert);


    }


}