package org.cnlp.genbank.db;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;

/**
 * Created with IntelliJ IDEA.
 * User: yatish
 * Date: 7/16/14
 * Time: 10:39 AM
 * To change this template use File | Settings | File Templates.
 */
public class DBManager
{
    public DBManager()
    {
    }

    public static Connection getConnection()
    {
        HashMap<String,String> properties = new HashMap<String, String>();
        Connection connection = null;
        BufferedReader reader = new BufferedReader(new InputStreamReader(DBManager.class.getClassLoader().getResourceAsStream("configuration.txt")));
        try
        {
            while (reader.ready())
            {
                String[] split = reader.readLine().split("=");
                properties.put(split[0], split[1]);
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

        if(null != properties.get("db-url") && null != properties.get("db-user") && null != properties.get("db-pass"))
        {
            try
            {
                connection = DriverManager.getConnection(properties.get("db-url"), properties.get("db-user"), properties.get("db-pass"));
            }
            catch (SQLException e)
            {
                e.printStackTrace();
            }
        }

        return connection;
    }
}
