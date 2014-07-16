import org.cnlp.genbank.db.DBManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by yatish on 2/19/14.
 */
public class JournalParserTest
{
    private static Logger logger = LoggerFactory.getLogger(JournalParserTest.class);

    /*private static final Pattern JOURNAL_PATTERN = Pattern.compile(
            "(?:.*\\(([\\d]{2})-([\\S]{3})-([\\d]{4})\\)([^,]*),(.*),(.*),([^\\(\\)]*))"
         + "|(?:.*\\(([\\d]{2})-([\\S]{3})-([\\d]{4})\\)([^,]*),(.*),(.*),([^\\(\\)]*)(?:(\\(E-mail:.*\\))|(\\(Tel:.*\\))|(\\(Fax:.*\\))|(URL.*)))"
    );*/
    private static final Pattern JOURNAL_PATTERN = Pattern.compile(
            "(?:Submitted[\\s]+\\(([\\d]{2})-([\\S]{3})-([\\d]{4})\\)([^,]*),(.*))"
    );

    /**
     * Patterns for zip code and country finding
     */
    private static final Pattern ZIP_USA = Pattern.compile(
            "(?:.*(?:([\\d]{5})|([\\d]{5}-[\\d]{4}))[,]?[\\s]?[Uu][\\s\\.]*[Ss][\\s\\.]*[Aa])"
    );
    private static final Pattern ZIP_INDIA = Pattern.compile(
            "(?:.*([\\d]{3}[\\s]?[\\d]{3})[\\s,]+(?:(?:INDIA)|(?:India)))"
    );
    private static final Pattern ZIP_CHINA = Pattern.compile(
            "(?:.*([\\d]{6}),[^,]*(?:(?:CHINA)|(?:China)))"
    );
    private static final Pattern ZIP_JAPAN = Pattern.compile(
            "(?:.*(?:([\\d]{3})|([\\d]{3}-[\\d]{1,4})),[\\s]*(?:(?:JAPAN)|(?:Japan)|(?:japan)))"
    );
    private static final Pattern ZIP_CANADA = Pattern.compile(
            "(?:.*([A-Z][0-9][A-Z]\\s[0-9][A-Z][0-9]),[\\s]*(?:(?:CANADA)|(?:Canada)|(?:canada)))"
    );
    private static final Pattern ZIP_UNITED_KINGDOM = Pattern.compile(
            "(?:.*([A-Z0-9]{3}\\s[A-Z0-9]{3})[\\s]?,[\\s]*(?:(?:UK)|(?:U\\.K)|(?:U\\.K\\.)|(?:United[\\s]?Kingdom)|(?:UNITED[\\s]?KINGDOM)|(?:england)|(?:England)|(?:ENGLAND)))"
    );
    private static final Pattern ZIP_FRANCE = Pattern.compile(
            "(?:.*([\\d]{5})[^,]*,[\\s]*[Ff][Rr][Aa][Nn][Cc][Ee])|"
          + "(?:.*([\\d]{5}).*[Ff][Rr][Aa][Nn][Cc][Ee])"
    );
    private static final Pattern ZIP_GERMANY = Pattern.compile(
            "(?:.*([\\d]{4,5})[^,]*,[^,]*[Gg][Ee][Rr][Mm][Aa][Nn][Yy])"
    );
    private static final Pattern ZIP_RUSSIA = Pattern.compile(
            "(?:.*([\\d]{6}).*[Rr][Uu][Ss][Ss][Ii][Aa])"
    );
    private static final Pattern ZIP_KOREA = Pattern.compile(
            "(?:.*([\\d]{3}-[\\d]{3}).*[Kk][Oo][Rr][Ee][Aa])"
    );
    private static final Pattern ZIP_DENMARK = Pattern.compile(
            "(?:.*([\\d]{4})[^,]*,[\\s]*[Dd][Ee][Nn][Mm][Aa][Rr][Kk])"
    );
    private static final Pattern ZIP_FRG = Pattern.compile(
            "(?:.*([\\d]{4})[^,]*,[\\s]*[Ff][\\.]?[\\s]?[Rr][\\.]?[\\s]?[Gg])"
    );
    private static final Pattern ZIP_ITALY = Pattern.compile(
            "(?:.*([\\d]{5})[^,]*[,]?[\\s]*[Ii][Tt][Aa][Ll][Yy])"
    );
    private static final Pattern ZIP_SPAIN = Pattern.compile(
            "(?:.*([\\d]{5})[^,]*[,]?[\\s]*[Ss][Pp][Aa][Ii][Nn])"
    );
    private static final Pattern ZIP_AUSTRALIA = Pattern.compile(
            "(?:.*([\\d]{4})[^,]*,[\\s]*[Aa][Uu][Ss][Tt][Rr][Aa][Ll][Ii][Aa])"
    );
    private static final Pattern ZIP_BELGIUM = Pattern.compile(
            "(?:.*([\\d]{4})[^,]*[,]?[\\s]*[Bb][Ee][Ll][Gg][Ii][Uu][Mm])"
    );
    private static final Pattern ZIP_ISRAEL = Pattern.compile(
            "(?:.*([\\d]{5})[^,]*[,]?[\\s]*[Ii][Ss][Rr][Aa][Ee][Ll])"
    );
    private static final Pattern ZIP_NORWAY = Pattern.compile(
            "(?:.*([\\d]{4})[^,]*[,]?[\\s]*[Nn][Oo][Rr][Ww][Aa][Yy])"
    );
    private static final Pattern ZIP_AUSTRIA = Pattern.compile(
            "(?:.*([\\d]{4})[^,]*[,]?[\\s]*[Aa][Uu][Ss][Tt][Rr][Ii][Aa])"
    );

    /**
     * Patterns for affiliations finding
     */
    private static final Pattern AFF_AUTHOR = Pattern.compile(
            "[\\s]*(.*(?:" +
                    "(?:[Dd][Ee][Pp][Aa][Rr][Tt][Mm][Ee][Nn][Tt])|" +
                    "(?:[Dd][Ii][Vv][Ii][Ss][Ii][Oo][Nn])|" +
                    "(?:[Uu][Nn][Ii][Vv][Ee][Rr][Ss][Ii][Tt][Yy])|" +
                    "(?:[Dd][Ee][Pp][Tt][\\.]?)" +
            ")[\\s]+[Oo][Ff].*)"
    );

    private static final Pattern AFF_AUTHOR_1 = Pattern.compile(
            "(.*[Uu][Nn][Ii][Vv][Ee][Rr][Ss][Ii][Tt][Yy].*)"
    );

    private static final Pattern AFF_AUTHOR_2 = Pattern.compile(
            "(.*[Ss][Cc][Hh][Oo][Oo][Ll].*)"
    );

    private static final Pattern AFF_AUTHOR_3 = Pattern.compile(
            "(.*[Uu][Nn][Ii][Vv][Ee][Rr][Ss][Ii][Tt][Aa][Ee][Tt].*)"
    );

    private static final Pattern AFF_AUTHOR_4 = Pattern.compile(
            "(.*[Ii][Nn][Ss][Tt][Ii][Tt][Uu][Tt][Ee]?.*)"
    );

    public static void main(String[] args) throws Exception
    {
        JournalParserTest genBankTest = new JournalParserTest();
        logger.info("Parsing started");
        long time = System.currentTimeMillis();

        //genBankTest.readReferenceCSV();

        //genBankTest.parseAddress();

        genBankTest.findAffiliation();

        logger.info(String.valueOf(
                (System.currentTimeMillis() - time) / 1000) + " seconds"
        );
        logger.info("Parsing Done");
    }

    private void findAffiliation() throws Exception
    {
        Connection connection = DBManager.getConnection();

        Map<String, String> map = null;
        map = new HashMap<String, String>();
        String query = "SELECT reference_id, author " +
                "FROM par_Reference_Journal " +
                "WHERE affiliation is NULL " +
                "AND author not like 'Contact:%' " +
                "AND author like '%institut%'";
        PreparedStatement statement = connection.prepareStatement(query);
        ResultSet resultSet = statement.executeQuery();
        while (resultSet.next())
        {
            String referenceId = resultSet.getString("reference_id");
            String author = resultSet.getString("author");
            map.put(referenceId, author);
        }
        statement.close();

        System.out.println(map.size() + " records found");
        int count = 0;
        List<String> mismatchList = new ArrayList<String>();
        for(Map.Entry<String, String> entry: map.entrySet())
        {
            String referenceId = entry.getKey();
            String author = entry.getValue();
            Matcher matcher = AFF_AUTHOR_4.matcher(author);
            if(matcher.matches())
            {
                String affiliation = matcher.group(1);

                query = "UPDATE par_Reference_Journal SET affiliation=\"" + affiliation + "\" WHERE reference_id='" + referenceId + "'";
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

    private void parseAddress() throws Exception
    {
        Connection connection = DBManager.getConnection();

        /*
        int startRecord = 0;
        int count = 1000;
        int totalRows = 687063;*/

        Map<String, String> map = null;
        //while ( startRecord < totalRows )
        //{
            map = new HashMap<String, String>();
            String query = "SELECT reference_id, address_1 FROM par_Reference_Journal WHERE address_1 like '%austria' AND zip is NULL";
            PreparedStatement statement = connection.prepareStatement(query);
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next())
            {
                String referenceId = resultSet.getString("reference_id");
                String address1 = resultSet.getString("address_1");
                map.put(referenceId, address1);
            }
            statement.close();

            //startRecord += count;
            System.out.println(map.size() + " records found");
            int count = 0;
            List<String> mismatchList = new ArrayList<String>();
            for(Map.Entry<String, String> entry: map.entrySet())
            {
                String referenceId = entry.getKey();
                String address1 = entry.getValue();
                Matcher matcher = ZIP_AUSTRIA.matcher(address1);
                if(matcher.matches())
                {
                    String zip = matcher.group(1);

                    if(null == zip)
                    {
                        zip = matcher.group(2);
                    }

                    //System.out.println(zip);

                    query = "UPDATE par_Reference_Journal SET zip='" + zip + "', country='AUSTRIA' WHERE reference_id='" + referenceId + "'";
                    statement = connection.prepareStatement(query);
                    statement.executeUpdate();
                    statement.close();
                }
                else
                {
                     mismatchList.add(referenceId);
                    //System.out.println(referenceId);
                    //System.out.println(address1);
                }
                count++;
                if(count%1000 == 0)
                {
                    System.out.println(count + " records processed");
                }
            }
            System.out.println(mismatchList.size() + " records mismatching the pattern");
        //}
        connection.close();
    }

    private void readReferenceCSV() throws Exception
    {
        BufferedReader br = new BufferedReader(new FileReader("C:/cygwin/home/yatish/projects/genbank/REFERENCE.CSV"));

        FileWriter fileWriter = new FileWriter("reference_journal_parse_error.csv");

        String line;

        while ((line = br.readLine()) != null)
        {
            String[] columns = line.split("\\|");

            String id = columns[0];

            // journal entry is the sixth entry in the CSV file
            if(columns.length < 6)
            {
                continue;
            }

            String title = columns[4];
            String journal = columns[5];

            // Trim the quotes and spaces surrounding the title and journal values
            title = title.replaceFirst("\"", "");
            title = title.endsWith("\"") ? title.substring(0, title.length() - 1) : title;
            title = title.trim();
            journal = journal.replaceFirst("\"", "");
            journal = journal.endsWith("\"") ? journal.substring(0, journal.length() - 1) : journal;
            journal = journal.trim();

            if(title.equalsIgnoreCase("Direct Submission"))
            {
                //System.out.println(journal);
                Matcher matcher = JOURNAL_PATTERN.matcher(journal);
                if(matcher.matches())
                {
                    String day = "";
                    String month = "";
                    String year = "";
                    String author = "";
                    String address1 = "";
                    String address2 = "";
                    String country = "";

                    day = matcher.group(1);
                    month = matcher.group(2);
                    year = matcher.group(3);
                    author = matcher.group(4).trim();
                    address1 = matcher.group(5).trim();

                    /*
                    System.out.println(day + " " + month + " " + year);
                    System.out.println(author);
                    System.out.println(address1);
                    System.out.println();
                    */

                    //writeReferenceJournal(id, day, month, year, author.trim(), address1.trim(), address2.trim(), country.trim());
                    writeReferenceJournal(id, day, month, year, author, address1);
                }
                else
                {
                    //logger.info(id + " journal pattern not matching. journal: " + journal);
                    fileWriter.write(id + "|" + journal);
                    fileWriter.write("\n");
                }
            }
        }

        fileWriter.close();

        br.close();
    }

    private void writeReferenceJournal(String id, String day, String month, String year, String author, String address1) throws Exception
    {
        Connection connection = DBManager.getConnection();
        String query = "INSERT INTO par_Reference_Journal (reference_id,day,month,year,author,address_1) values(?,?,?,?,?,?)";
        PreparedStatement statement = connection.prepareStatement(query);
        statement.setLong(1, Long.parseLong(id));
        statement.setInt(2, Integer.parseInt(day));
        statement.setString(3, month);
        statement.setInt(4, Integer.parseInt(year));
        statement.setString(5, author);
        statement.setString(6, address1);
        statement.executeUpdate();
        statement.close();
        connection.close();
    }

    private void writeReferenceJournal(String id, String day, String month, String year, String author, String address1, String address2, String country) throws Exception
    {
        Connection connection = DBManager.getConnection();
        String query = "INSERT INTO par_Reference_Journal (reference_id,day,month,year,author,address_1,address_2,country) values(?,?,?,?,?,?,?,?)";
        PreparedStatement statement = connection.prepareStatement(query);
        statement.setLong(1, Long.parseLong(id));
        statement.setInt(2, Integer.parseInt(day));
        statement.setString(3, month);
        statement.setInt(4, Integer.parseInt(year));
        statement.setString(5, author);
        statement.setString(6, address1);
        statement.setString(7, address2);
        statement.setString(8, country);
        statement.executeUpdate();
        statement.close();
        connection.close();
    }

    private void readReference() throws Exception
    {
        Connection connection = DBManager.getConnection();
        Statement statement = connection.createStatement();
        String query = "SELECT id, journal from Reference WHERE title='Direct Submission' ORDER BY id ASC LIMIT 10";
        ResultSet resultSet = statement.executeQuery(query);
        while(resultSet.next())
        {
            String id = resultSet.getString("id");
            String journal = resultSet.getString("journal");
            String[] splits = journal.split(",");
            if(splits.length > 2)
            {
                String dateAndAuthor = splits[0];
                String country = splits[splits.length-1];
                String address2 = splits[splits.length-2];

                Pattern pattern = Pattern.compile(".*\\(([\\d]{2})-([\\S]{3})-([\\d]{4})\\)(.*)");
                Matcher matcher = pattern.matcher(dateAndAuthor);
                String day = "";
                String month = "";
                String year = "";
                String author = "";
                if(matcher.matches())
                {
                    day = matcher.group(1);
                    month = matcher.group(2);
                    year = matcher.group(3);
                    author = matcher.group(4);
                }

                String address1 = "";
                for(int i=1; i<splits.length-3; i++)
                {
                    address1 += splits[i] + ",";
                }
                address1 += splits[splits.length-3];

                System.out.println(day + " " + month + " " + year + " " + author);
                System.out.println(address1);
                System.out.println(address2);
                System.out.println(country);
                System.out.println();

                writeReferenceJournal(id, day, month, year, author.trim(), address1.trim(), address2.trim(), country.trim());
            }
        }
        resultSet.close();
        statement.close();
        connection.close();
    }
}
