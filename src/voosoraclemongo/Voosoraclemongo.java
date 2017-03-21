package voosoraclemongo;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.bson.Document;

public class Voosoraclemongo {

    public static Connection conn = null;

    public static void Conexion() throws SQLException {

        String driver = "jdbc:oracle:thin:";
        String host = "localhost.localdomain";
        String porto = "1521";
        String sid = "orcl";
        String usuario = "hr";
        String password = "hr";
        String url = driver + usuario + "/" + password + "@" + host + ":" + porto + ":" + sid;
        conn = DriverManager.getConnection(url);

        if (conn != null) {

            System.out.println("Conexion realizada");

        } else {

            System.out.println("Erro ao conectar");
        }
    }

    public static void Cerrar() throws SQLException {
        conn.close();

    }

    public static void main(String[] args) throws SQLException {
        Voosoraclemongo vs = new Voosoraclemongo();
        vs.Conexion();
        vs.Metodos();
    }

    public void Metodos() throws SQLException {

        MongoClient cliente = new MongoClient("localhost", 27017);

        //acceder a base 
        MongoDatabase base = cliente.getDatabase("internacional");

        //acceder a unha coleccion
        MongoCollection<Document> coleccion = base.getCollection("reserva");

        FindIterable<Document> cursor = coleccion.find();

        MongoCursor<Document> iterator = cursor.iterator();

        while (iterator.hasNext()) {

            Document d = iterator.next();
            Double codr = d.getDouble("codr");
            String dni = d.getString("dni");
            Double idVooIda = d.getDouble("idvooida");
            Double idVooVolta = d.getDouble("idvoovolta");
            Double prezoReserva = d.getDouble("prezoreserva");

            int confirmado = d.getInteger("confirmado");

            System.out.println("CODIGO " + codr + " DNI " + dni + " IDA " + idVooIda + "VOLTA " + idVooVolta + "PREZO " + prezoReserva + " CONFIRMADO " + confirmado);

            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery("Select nreservas from pasaxeiros where DNI='" + dni + "'");
            while (rs.next()) {

                int reserva = rs.getInt(1) + 1;

                System.out.println("NRESERVA: " + reserva);

                //st.executeUpdate("UPDATE pasaxeiros SET nreservas=" + reserva + " WHERE dni='" + dni + "'");
                //System.out.println("UPDATE FINALIZADO");
            }
            
            int prezovolta = 0;
            int prezoida = 0;
            int total ;
            ResultSet rs2 = st.executeQuery("Select prezo from voos where voo=" + idVooIda);

            while (rs2.next()) {

                prezoida = rs2.getInt(1);

                System.out.println(prezoida);
            }
            ResultSet rs3 = st.executeQuery("Select prezo from voos where voo=" + idVooVolta);
            while (rs3.next()) {

                prezovolta = rs3.getInt(1);

                System.out.println(prezovolta);
            }

            total = prezoida + prezovolta;

            coleccion.updateOne(new Document("codr",codr),new Document("$set",new Document("prezoreserva",total)));

        }

    }
}
