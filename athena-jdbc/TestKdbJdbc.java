import java.sql.*;
import java.util.*;

public class TestKdbJdbc {
	public static void main(String[] args) throws Exception {
		Class.forName("jdbc");
		Connection h = DriverManager.getConnection("jdbc:q:127.0.0.1:5001", "", "");
		System.out.println("connected. v1");
		Statement stmt = h.createStatement();
		ResultSet rs = stmt.executeQuery("select * from t2");
		while(rs.next()) {
			Object obj = rs.getObject(1);
			System.out.println(obj + (obj == null ? "null" : obj.getClass().getName()));
		}
	}
}
