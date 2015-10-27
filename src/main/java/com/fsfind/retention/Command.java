package com.fsfind.retention;

import java.io.PrintWriter;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * Simple command line abstraction.
 */
public abstract class Command {

  public static final int SUCCESS = 0;
  public static final int FAILURE = -1;

  private String usageLine = getClass().getSimpleName() + " <options>";
  private String usageHeader = "Options:";
  private String usageFooter = "";

  public abstract Options buildOptions();

  /**
   * CommandLine's application logic goes here.
   *
   * @param cl the command line arguments
   * @return SUCCESS if command runs fine, FAILURE otherwise
   */
  public abstract int run(CommandLine cl) throws Exception;

  public CommandLine parseOptions(Options options, String[] args)
      throws ParseException {
    CommandLineParser parser = new GnuParser();
    CommandLine cmd = parser.parse(options, args);
    return cmd;
  }

  /**
   * Invoke this from your 'public static void main'.
   *
   * @param args the argument array passed to main end point
   * @return SUCCESS if command runs fine, FAILURE otherwise
   */
  public int doMain(String[] args) throws Exception {
    Options options = buildOptions();
    CommandLine cl;
    try {
      cl = parseOptions(options, args);
    } catch (ParseException e) {
      System.err.println("Error parsing options: " + e.getMessage());
      printCommandUsage(options);
      return FAILURE;
    }
    return run(cl);
  }

  private void printCommandUsage(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    PrintWriter pw = new PrintWriter(System.out);
    formatter.printHelp(pw, 70, usageLine, usageHeader, options, 2,
        HelpFormatter.DEFAULT_DESC_PAD, null, false);
    pw.flush();
    System.out.println(usageFooter);
  }

}
