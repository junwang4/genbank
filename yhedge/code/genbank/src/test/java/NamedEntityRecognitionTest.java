import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.namefind.TokenNameFinderModel;
import opennlp.tools.util.Span;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Created by yatish on 3/11/14.
 */
public class NamedEntityRecognitionTest
{
    public static void main(String[] args) throws Exception
    {
        InputStream modelIn = NamedEntityRecognitionTest.class.getResourceAsStream("en-ner-location.bin");

        TokenNameFinderModel model = new TokenNameFinderModel(modelIn);

        NameFinderME nameFinder = new NameFinderME(model);

        String document = "Dept of Biochemistry Temple University 3400 North Broad Street Philadelphia PA 19140 USA";

        String[] sentence = document.split("\\s");

        for(String s: sentence)
        {
            System.out.println(s);
        }

        Span nameSpans[] = nameFinder.find(sentence);

        for(Span span: nameSpans)
        {
            System.out.println(document.substring(span.getStart(), span.getEnd()));
            System.out.println(span.toString());
        }

        nameFinder.clearAdaptiveData();

    }
}
