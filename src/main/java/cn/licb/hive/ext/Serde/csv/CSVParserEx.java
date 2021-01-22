package cn.licb.hive.ext.Serde.csv;

/**
 Copyright 2005 Bytecode Pty Ltd.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A very simple CSV parser released under a commercial-friendly license.
 * This just implements splitting a single line into fields.
 * 基于Hive中的CSVParser修改支持多字符的CSVParser
 *
 * @author Glen Smith
 * @author Rainer Pruy
 * @author 李宠波
 */
public class CSVParserEx {

    private final char[] separator;

    private final char[] quotechar;

    private final char escape;

    private final boolean strictQuotes;

    private String pending;
    private boolean inField = false;

    private final boolean ignoreLeadingWhiteSpace;

    /**
     * The default separator to use if none is supplied to the constructor.
     */
    public static final String DEFAULT_SEPARATOR = ",";

    public static final int INITIAL_READ_SIZE = 128;

    /**
     * The default quote character to use if none is supplied to the
     * constructor.
     */
    public static final String DEFAULT_QUOTE_CHARACTER = "\"";


    /**
     * The default escape character to use if none is supplied to the
     * constructor.
     */
    public static final char DEFAULT_ESCAPE_CHARACTER = '\\';

    /**
     * The default strict quote behavior to use if none is supplied to the
     * constructor
     */
    public static final boolean DEFAULT_STRICT_QUOTES = false;

    /**
     * The default leading whitespace behavior to use if none is supplied to the
     * constructor
     */
    public static final boolean DEFAULT_IGNORE_LEADING_WHITESPACE = true;

    /**
     * This is the "null" character - if a value is set to this then it is ignored.
     * I.E. if the quote character is set to null then there is no quote character.
     */
    public static final char NULL_CHARACTER = '\0';

    /**
     * Constructs CSVParser using a comma for the separator.
     */
    public CSVParserEx() {
        this(DEFAULT_SEPARATOR, DEFAULT_QUOTE_CHARACTER, DEFAULT_ESCAPE_CHARACTER);
    }

    /**
     * Constructs CSVParser with supplied separator.
     *
     * @param separator the delimiter to use for separating entries.
     */
    public CSVParserEx(String separator) {
        this(separator, DEFAULT_QUOTE_CHARACTER, DEFAULT_ESCAPE_CHARACTER);
    }


    /**
     * Constructs CSVParser with supplied separator and quote char.
     *
     * @param separator the delimiter to use for separating entries
     * @param quotechar the character to use for quoted elements
     */
    public CSVParserEx(String separator, String quotechar) {
        this(separator, quotechar, DEFAULT_ESCAPE_CHARACTER);
    }

    /**
     * Constructs CSVReader with supplied separator and quote char.
     *
     * @param separator the delimiter to use for separating entries
     * @param quotechar the character to use for quoted elements
     * @param escape    the character to use for escaping a separator or quote
     */
    public CSVParserEx(String separator, String quotechar, char escape) {
        this(separator, quotechar, escape, DEFAULT_STRICT_QUOTES);
    }

    /**
     * Constructs CSVReader with supplied separator and quote char.
     * Allows setting the "strict quotes" flag
     *
     * @param separator    the delimiter to use for separating entries
     * @param quotechar    the character to use for quoted elements
     * @param escape       the character to use for escaping a separator or quote
     * @param strictQuotes if true, characters outside the quotes are ignored
     */
    public CSVParserEx(String separator, String quotechar, char escape, boolean strictQuotes) {
        this(separator, quotechar, escape, strictQuotes, DEFAULT_IGNORE_LEADING_WHITESPACE);
    }

    /**
     * Constructs CSVReader with supplied separator and quote char.
     * Allows setting the "strict quotes" and "ignore leading whitespace" flags
     *
     * @param sep               the delimiter to use for separating entries
     * @param quote               the character to use for quoted elements
     * @param escape                  the character to use for escaping a separator or quote
     * @param strictQuotes            if true, characters outside the quotes are ignored
     * @param ignoreLeadingWhiteSpace if true, white space in front of a quote in a field is ignored
     */
    public CSVParserEx(String sep, String quote, char escape, boolean strictQuotes, boolean ignoreLeadingWhiteSpace) {
        char[] separator = sep.toCharArray();
        char[] quotechar = quote.toCharArray();
        if (anyCharactersAreTheSame(separator, quotechar, escape)) {
            throw new UnsupportedOperationException("The separator, quote, and escape characters must be different!");
        }
        if (separator.length == 0 && separator[0] == NULL_CHARACTER) {
            throw new UnsupportedOperationException("The separator character must be defined!");
        }
        this.separator = separator;
        this.quotechar = quotechar;
        this.escape = escape;
        this.strictQuotes = strictQuotes;
        this.ignoreLeadingWhiteSpace = ignoreLeadingWhiteSpace;
    }

    private boolean anyCharactersAreTheSame(char[] separator, char[] quotechar, char escape) {
        return isSameCharacter(separator[0], quotechar[0]) || isSameCharacter(separator[0], escape) || isSameCharacter(quotechar[0], escape);
    }

    private boolean isSameCharacter(char c1, char c2) {
        return c1 != NULL_CHARACTER && c1 == c2;
    }

    /**
     * @return true if something was left over from last call(s)
     */
    public boolean isPending() {
        return pending != null;
    }

    public String[] parseLineMulti(String nextLine) throws IOException {
        return parseLine(nextLine, true);
    }

    public String[] parseLine(String nextLine) throws IOException {
        return parseLine(nextLine, false);
    }

    /**
     * Parses an incoming String and returns an array of elements.
     *
     * @param nextLine the string to parse
     * @param multi
     * @return the comma-tokenized list of elements, or null if nextLine is null
     * @throws IOException if bad things happen during the read
     */
    private String[] parseLine(String nextLine, boolean multi) throws IOException {

        if (!multi && pending != null) {
            pending = null;
        }

        if (nextLine == null) {
            if (pending != null) {
                String s = pending;
                pending = null;
                return new String[]{s};
            } else {
                return null;
            }
        }

        List<String> tokensOnThisLine = new ArrayList<String>();
        StringBuilder sb = new StringBuilder(INITIAL_READ_SIZE);
        boolean inQuotes = false;
        if (pending != null) {
            sb.append(pending);
            pending = null;
            inQuotes = true;
        }
        for (int i = 0; i < nextLine.length(); i++) {

            char c = nextLine.charAt(i);
            if (c == this.escape) {
                if (isNextCharacterEscapable(nextLine, inQuotes || inField, i)) {
                    sb.append(nextLine.charAt(i + 1));
                    i++;
                }
            } else if (isQuoteChars(nextLine, i)) {
                if (isNextCharacterEscapedQuote(nextLine, inQuotes || inField, i + quotechar.length)) {
                    //连续2个quote，就保留一个
                    sb.append(nextLine.charAt(i + quotechar.length));
                    i += quotechar.length;
                } else {
                    //inQuotes = !inQuotes;

                    // the tricky case of an embedded quote in the middle: a,bc"d"ef,g
                    if (!strictQuotes) {
                        if (i > quotechar.length //not on the beginning of the line
                                && nextLine.length() > (i + separator.length)
                                && !isSeparatorChars(nextLine, i - separator.length) //not at the beginning of an escape sequence
                                && !isSeparatorChars(nextLine, i + quotechar.length) //not at the	end of an escape sequence
                        ) {
                            //quote前不是紧跟着separator,   并且  后面没有紧跟着quote
                            if (ignoreLeadingWhiteSpace && sb.length() > 0 && isAllWhiteSpace(sb)) {
                                sb.setLength(0);  //discard white space leading up to quote
                            } else {
                                for(int j=0;j<quotechar.length;j++) {
                                    sb.append(nextLine.charAt(i+j));
                                }
                                //continue;
                            }

                        }
                    }
                    i += quotechar.length - 1;
                    inQuotes = !inQuotes;
                }
                inField = !inField;
            } else if (isSeparatorChars(nextLine, i) && !inQuotes) {
                tokensOnThisLine.add(sb.toString());
                sb.setLength(0); // start work on next token
                i += separator.length - 1;
                inField = false;
            } else {
                if (!strictQuotes || inQuotes) {
                    sb.append(c);
                    inField = true;
                }
            }
        }
        // line is done - check status
        if (inQuotes) {
            if (multi) {
                // continuing a quoted section, re-append newline
                sb.append("\n");
                pending = sb.toString();
                sb = null; // this partial content is not to be added to field list yet
            } else {
                throw new IOException("Un-terminated quoted field at end of CSV line");
            }
        }
        if (sb != null) {
            tokensOnThisLine.add(sb.toString());
        }
        return tokensOnThisLine.toArray(new String[tokensOnThisLine.size()]);

    }

    /**
     * precondition: the current character is a quote
     *
     * @param nextLine the current line
     * @param inQuotes true if the current context is quoted
     * @param i        current index in line  (after the current quote)
     * @return true if the following character is a quote
     */
    private boolean isNextCharacterEscapedQuote(String nextLine, boolean inQuotes, int i) {
        return inQuotes  // we are in quotes, therefore there can be escaped quotes in here.
                && nextLine.length() > (i + quotechar.length)  // there is indeed another character to check.
                && isQuoteChars(nextLine, i);
    }

    private boolean isQuoteChars(String nextLine, int beginIndex){
        for(int j = 0;j<quotechar.length;j++){
            if (nextLine.charAt(j + beginIndex) != quotechar[j])
                return false;
        }
        return true;
    }

    private boolean isSeparatorChars(String nextLine, int beginIndex){
        for(int j = 0;j<separator.length;j++){
            if (nextLine.charAt(j + beginIndex) != separator[j])
                return false;
        }
        return true;
    }

    /**
     * precondition: the current character is an escape
     *
     * @param nextLine the current line
     * @param inQuotes true if the current context is quoted
     * @param i        current index in line
     * @return true if the following character is a quote
     */
    protected boolean isNextCharacterEscapable(String nextLine, boolean inQuotes, int i) {
        return inQuotes  // we are in quotes, therefore there can be escaped quotes in here.
                && nextLine.length() > (i + quotechar.length)  // there is indeed another character to check.
                && (isQuoteChars(nextLine, i + 1) || nextLine.charAt(i + 1) == this.escape);
    }

    /**
     * precondition: sb.length() > 0
     *
     * @param sb A sequence of characters to examine
     * @return true if every character in the sequence is whitespace
     */
    protected boolean isAllWhiteSpace(CharSequence sb) {
        boolean result = true;
        for (int i = 0; i < sb.length(); i++) {
            char c = sb.charAt(i);

            if (!Character.isWhitespace(c)) {
                return false;
            }
        }
        return result;
    }
}
