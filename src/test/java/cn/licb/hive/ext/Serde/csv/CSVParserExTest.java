package cn.licb.hive.ext.Serde.csv;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 使用单字符、多字符分别进行测试
 *
 * @author Chongbo Li at 2021/1/22
 */
class CSVParserExTest {

    @Test
    @DisplayName("single char")
    void parseLineForSingleChar() throws IOException {
        assertStringArray(new String[]{"aaa,111", "bbbb", "cccc\"3333"},
                parseCSV(",", "\"", '\\', "\"aaa\\,111\",\"bbbb\",\"cccc\\\"3333\""));

        assertStringArray(new String[]{"aaaaaaaaaaaaaa", "bbbb", "ccccccccccccc"},
                parseCSV(",", "\"", '\\', "aaaaaaaaaaaaaa,bbbb,\"ccccccccccccc\""));

        assertStringArray(new String[]{"aaaaa\"1111", "b\"b\"b", "ccccccccccccc"},
                parseCSV(",", "\"", '\\', "aaaaa\"\"1111,b\"b\"b,\"ccccccccccccc\""));

        assertStringArray(new String[]{"aaaaa\"1111", "b\"b\"b", "cccccc\nccccccc"},
                parseCSV(",", "\"", '\\', "aaaaa\"\"1111,b\"b\"b,\"cccccc\nccccccc\""));
    }

    @Test
    @DisplayName("multi char")
    void parseLineForMultiChar() throws IOException {
        assertStringArray(new String[]{"aaa||111", "bbbb", "cccc#~3333"},
                parseCSV("||", "#~", '\\', "#~aaa\\||111#~||#~bbbb#~||#~cccc\\#~3333#~"));

        assertStringArray(new String[]{"aaaaaaaaaaaaaa", "bbbb", "ccccccccccccc"},
                parseCSV("||", "#~", '\\', "aaaaaaaaaaaaaa||bbbb||#~ccccccccccccc#~"));

        assertStringArray(new String[]{"aaaaa#~1111", "b#~b#~b", "ccccccccccccc"},
                parseCSV("||", "#~", '\\', "aaaaa#~#~1111||b#~b#~b||#~ccccccccccccc#~"));

        assertStringArray(new String[]{"aaaaa#~1111", "b#~b#~b", "cccccc\nccccccc"},
                parseCSV("||", "#~", '\\', "aaaaa#~#~1111||b#~b#~b||#~cccccc\nccccccc#~"));
    }

    private static String[] parseCSV(String separator, String quote, char escape, String str) throws IOException {
        return new CSVParserEx(separator, quote, escape).parseLine(str);
    }

    private static void assertStringArray(String[] expected, String[] actual){
        assertEquals(expected.length, actual.length, "length is equal");
        for(int i=0;i<expected.length;i++){
            assertEquals(expected[i], actual[i], String.format("index %d is equal", i));
        }
    }
}