import org.cnlp.genbank.db.DBManager;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by yatish on 4/2/14.
 */
public class Test
{

    private static final Pattern COUNTRY = Pattern.compile(
            ".*(?:" +
            "((?:[Uu][\\.]?[\\s]?[Ss][\\.]?[\\s]?[Aa]))|" +        /* 1. USA */
            "([Cc][Aa][Nn][Aa][Dd][Aa])|" +                        /* 2. CANADA */
            "([Uu][\\.]?[\\s]?[Kk])|" +                            /* 3. UK */
            "([Cc][Hh][Ii][Nn][Aa])|" +                            /* 4. CHINA */
            "((?:[Nn][Ee][Tt][Hh][Ee][Rr][Ll][Aa][Nn][Dd][Ss]?)|(?:[Tt][Hh][Ee][\\s]?[Nn][Ee][Tt][Hh][Ee][Rr][Ll][Aa][Nn][Dd][Ss]?))|" +   /* 5. NETHERLANDS */
            "([Pp][Oo][Ll][Aa][Nn][Dd])|" +                        /* 6. POLAND */
            "([Aa][Uu][Ss][Tt][Rr][Aa][Ll][Ii][Aa])|" +            /* 7. AUSTRALIA */
            "([Ss][Ww][Ii][Tt][Zz][Ee][Rr][Ll][Aa][Nn][Dd])|" +    /* 8. SWITZERLAND */
            "([Nn][Ee][Ww][\\s]?[Zz][Ee][Aa][Ll][Aa][Nn][Dd])|" +     /* 9. NEW ZEALAND */
            "([Ss][Ww][Ee][Dd][Ee][Nn])|" +                           /* 10. SWEDEN */
            "([Kk][Oo][Rr][Ee][Aa])|" +                               /* 11. KOREA */
            "([Bb][Ee][Ll][Gg][Ii][Uu][Mm])|" +                       /* 12. BELGIUM */
            "([Jj][Aa][Pp][Aa][Nn])|" +                               /* 13. JAPAN */
            "([Ss][Cc][Oo][Tt][Ll][Aa][Nn][Dd])|" +                   /* 14. SCOTLAND */
            "([Ff][Ii][Nn][Ll][Aa][Nn][Dd])|" +                       /* 15. FINLAND */
            "([Gg][Ee][Rr][Mm][Aa][Nn][Yy])|" +                       /* 16. GERMANY */
            "([Gg][Rr][Ee][Ee][Cc][Ee])" +                           /* 17. GREECE */
            ")"
    );

    private static final Pattern ONE_COUNTRY = Pattern.compile(
            ".*botswana", Pattern.CASE_INSENSITIVE
    );
    private static final String COUNTRY_NAME = "BOTSWANA";

    public static void main(String[] args) throws Exception
    {
        Test test = new Test();
        //test.test1();
        test.getAffiliation();
    }

    private static final Pattern AFFILIATION = Pattern.compile("d" +
            "[\\s]*(department of[^,]*,university of[^,]*,).*", Pattern.CASE_INSENSITIVE
    );
    private void getAffiliation() throws Exception
    {
        Map<String, String> map = getAddressAffiliationNull();
        System.out.println(map.size() + " records found");
        int count = 0;
        List<String> mismatchList = new ArrayList<String>();
        String referenceId;
        String address;
        Connection connection = DBManager.getConnection();
        for(Map.Entry<String, String> entry: map.entrySet())
        {
            referenceId = entry.getKey();
            address = entry.getValue();
            Matcher matcher = AFFILIATION.matcher(address);
            if(matcher.matches())
            {
                updateAffiliation(connection, referenceId, matcher.group(1));
            }
            else
            {
                mismatchList.add(referenceId);
            }
            count++;
            if( (count%1000) == 0 )
            {
                System.out.println(count + " records processed");
            }
        }
        System.out.println(mismatchList.size() + " records mismatching the pattern");
        connection.close();
    }

    private Map<String, String> getAddressAffiliationNull() throws Exception
    {
        Map<String, String> map = new HashMap<String, String>();

        Connection connection = DBManager.getConnection();

        String query = "SELECT reference_id, address_1 " +
                "FROM par_Reference_Journal " +
                "WHERE affiliation is NULL";

        PreparedStatement statement = connection.prepareStatement(query);

        ResultSet resultSet = statement.executeQuery();

        while (resultSet.next())
        {
            map.put(resultSet.getString("reference_id"), resultSet.getString("address_1"));
        }

        statement.close();

        connection.close();

        return map;
    }

    private void updateAffiliation(Connection connection, String referenceId, String affiliation) throws Exception
    {
        String query = "UPDATE par_Reference_Journal SET affiliation=\"" + affiliation + "\" WHERE reference_id='" + referenceId + "'";

        PreparedStatement statement = connection.prepareStatement(query);

        statement.executeUpdate();

        statement.close();
    }

    private void test1() throws Exception
    {
        Map<String, String> map = getAddressCountryNull();
        System.out.println(map.size() + " records found");
        int count = 0;
        List<String> mismatchList = new ArrayList<String>();
        String referenceId;
        String address;
        Connection connection = DBManager.getConnection();
        for(Map.Entry<String, String> entry: map.entrySet())
        {
            referenceId = entry.getKey();
            address = entry.getValue();
            Matcher matcher = ONE_COUNTRY.matcher(address);
            if(matcher.matches())
            {
                updateCountry(connection, referenceId, COUNTRY_NAME);
            }
            else
            {
                mismatchList.add(referenceId);
            }
            count++;
            if( (count%1000) == 0 )
            {
                System.out.println(count + " records processed");
            }
        }
        System.out.println(mismatchList.size() + " records mismatching the pattern");
        connection.close();
    }

    private Map<String, String> getAddressCountryNull() throws Exception
    {
        Map<String, String> map = new HashMap<String, String>();

        Connection connection = DBManager.getConnection();

        String query = "SELECT reference_id, address_1 " +
                       "FROM par_Reference_Journal " +
                       "WHERE country is NULL ";

        PreparedStatement statement = connection.prepareStatement(query);

        ResultSet resultSet = statement.executeQuery();

        while (resultSet.next())
        {
            map.put(resultSet.getString("reference_id"), resultSet.getString("address_1"));
        }

        statement.close();

        connection.close();

        return map;
    }

    private void updateCountry(Connection connection, String referenceId, String country) throws Exception
    {
        String query = "UPDATE par_Reference_Journal SET country=\"" + country + "\" WHERE reference_id='" + referenceId + "'";

        PreparedStatement statement = connection.prepareStatement(query);

        statement.executeUpdate();

        statement.close();
    }

    private void Test2() throws Exception
    {
        Connection connection = DBManager.getConnection();

        Map<String, String> map = null;
        map = new HashMap<String, String>();
        String query = "SELECT reference_id, address_1 " +
                "FROM par_Reference_Journal " +
                "WHERE country is NULL ";
        PreparedStatement statement = connection.prepareStatement(query);
        ResultSet resultSet = statement.executeQuery();
        while (resultSet.next())
        {
            map.put(resultSet.getString("reference_id"), resultSet.getString("address_1"));
        }
        statement.close();

        System.out.println(map.size() + " records found");
        int count = 0;
        List<String> mismatchList = new ArrayList<String>();
        String referenceId;
        String address;
        String country = null;
        for(Map.Entry<String, String> entry: map.entrySet())
        {
            referenceId = entry.getKey();
            address = entry.getValue();
            Matcher matcher = COUNTRY.matcher(address);
            if(matcher.matches())
            {
                if(null != matcher.group(1))
                {
                    country = "USA";
                }
                else if(null != matcher.group(2))
                {
                    country = "CANADA";
                }
                else if(null != matcher.group(3))
                {
                    country = "UK";
                }
                else if(null != matcher.group(4))
                {
                    country = "CHINA";
                }
                else if(null != matcher.group(5))
                {
                    country = "NETHERLANDS";
                }
                else if(null != matcher.group(6))
                {
                    country = "POLAND";
                }
                else if(null != matcher.group(7))
                {
                    country = "AUSTRALIA";
                }
                else if(null != matcher.group(8))
                {
                    country = "SWITZERLAND";
                }
                else if(null != matcher.group(9))
                {
                    country = "NEW ZEALAND";
                }
                else if(null != matcher.group(10))
                {
                    country = "SWEDEN";
                }
                else if(null != matcher.group(11))
                {
                    country = "KOREA";
                }
                else if(null != matcher.group(12))
                {
                    country = "BELGIUM";
                }
                else if(null != matcher.group(13))
                {
                    country = "JAPAN";
                }
                else if(null != matcher.group(14))
                {
                    country = "SCOTLAND";
                }
                else if(null != matcher.group(15))
                {
                    country = "FINLAND";
                }
                else if(null != matcher.group(16))
                {
                    country = "GERMANY";
                }
                else if(null != matcher.group(17))
                {
                    country = "GREECE";
                }

                query = "UPDATE par_Reference_Journal SET country=\"" + country + "\" WHERE reference_id='" + referenceId + "'";
                statement = connection.prepareStatement(query);
                statement.executeUpdate();
                statement.close();
            }
            else
            {
                mismatchList.add(referenceId);
            }
            count++;
            if(count%1000 == 0)
            {
                System.out.println(count + " records processed");
            }
        }
        System.out.println(mismatchList.size() + " records mismatching the pattern");

        connection.close();
    }
}
