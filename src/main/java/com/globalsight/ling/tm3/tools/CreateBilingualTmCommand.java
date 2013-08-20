package com.globalsight.ling.tm3.tools;

import java.io.PrintStream;
import java.util.Collections;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.hibernate.Session;

import com.globalsight.ling.tm3.core.TM3Locale;
import com.globalsight.ling.tm3.core.TM3Tm;

@SuppressWarnings("static-access")
class CreateBilingualTmCommand extends CreateTmCommand {

    @Override
    public String getName() {
        return "create-bilingual";
    }
    
    @Override
    public String getDescription() {
        return "create a new TM";
    }
    
    @Override
    protected String getUsageLine() {
        return getName() + " [options] -" + SOURCE + " srcLocale -" + TARGET +
                " tgtLocale";
    }
    
    @Override
    protected void printExtraHelp(PrintStream out) {
        out.println("Creates a new bilingual TM with the given source and target locale.");
        out.println("Locales should be specified as xx_YY codes.");
    }

    static final String SOURCE = "source";
    static final Option SOURCE_OPT = OptionBuilder.withArgName("locale")
                        .hasArg()
                        .withDescription("source locale code (xx_YY)")
                        .isRequired()
                        .create(SOURCE);
    static final String TARGET = "target";
    static final Option TARGET_OPT = OptionBuilder.withArgName("locale")
                        .hasArg()
                        .withDescription("target locale code (xx_YY)")
                        .isRequired()
                        .create(TARGET);
                        
    @Override
    public Options getOptions() {
        Options opts = getDefaultOptions();
        opts.addOption(SOURCE_OPT);
        opts.addOption(TARGET_OPT);
        return opts;
    }
    
    @Override
    protected boolean requiresDataFactory() {
        return true;
    }

    @Override
    public TM3Tm<?> createTm(Session session, CommandLine command) 
                          throws Exception {     
        TM3Locale srcLocale = getDataFactory().getLocaleByCode(session, 
                                            command.getOptionValue(SOURCE));
        if (srcLocale == null) {
            die("'" + command.getOptionValue(SOURCE) + 
                    "' is not a valid locale code for -" + SOURCE);
        }
        TM3Locale tgtLocale = getDataFactory().getLocaleByCode(session, 
                                            command.getOptionValue(TARGET));
        if (tgtLocale == null) {
            die("'" + command.getOptionValue(TARGET) + 
                    "' is not a valid locale code for -" + TARGET);
        }
        
        return getManager().createBilingualTm(
                null, Collections.EMPTY_SET,
                srcLocale, tgtLocale);
    }
    
}
