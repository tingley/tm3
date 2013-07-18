package com.globalsight.ling.tm3.tools;

import java.io.PrintStream;
import java.util.Formatter;
import java.util.List;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.hibernate.Session;

import com.globalsight.ling.tm3.core.TM3Attribute;
import com.globalsight.ling.tm3.core.TM3BilingualTm;
import com.globalsight.ling.tm3.core.TM3Data;
import com.globalsight.ling.tm3.core.TM3DataFactory;
import com.globalsight.ling.tm3.core.TM3SharedTm;
import com.globalsight.ling.tm3.core.TM3Tm;

// Usage:
// show       # shows all 
// show [id]  # detailed info about a since one
@SuppressWarnings("unchecked")
class ShowTmCommand extends TM3Command {

    @Override
    public String getDescription() {
        return "print a list of all TM3 memories";
    }

    @Override
    public String getName() {
        return "show";
    }
    
    @Override
    protected void printExtraHelp(PrintStream out) {
        out.println("Displays all TM3 TMs.");
    }

    @Override
    protected boolean requiresDataFactory() {
        return true;
    }
    
    @Override
    protected void handle(Session session, CommandLine command)
            throws Exception {
        Formatter f = new Formatter(System.out);
        
        List<String> args = command.getArgList();
        if (args.size() == 0) {
            TM3DataFactory factory = getDataFactory();
            showAll(getManager().getAllTms(session, factory), f);
        }
        else {
            for (String a : args) {
                TM3Tm tm = getTm(session, a);
                if (tm == null) {
                    System.err.println("Skipping '" +a+ "' - not a valid id");
                    continue;
                }
                showOne(tm, f);
                System.out.println("");
            }
        }
        f.flush();
    }
    
    private <T extends TM3Data> void showAll(List<TM3Tm<T>> tms, Formatter f) throws Exception {
        if (tms.size() == 0) {
            return;
        }
        f.format("%-12s%s\n", "Id", "Type");
        for (TM3Tm tm : tms) {
            switch (tm.getType()) {
            case BILINGUAL:
                TM3BilingualTm btm = (TM3BilingualTm)tm;
                f.format("%-12d%s (%s -> %s)\n", btm.getId(), btm.getType(), 
                         btm.getSrcLocale().getLocaleCode(),
                         btm.getTgtLocale().getLocaleCode());
                break;
            default:
                f.format("%-12d%s\n", tm.getId(), tm.getType());
            }
        }
    }

    private void showOne(TM3Tm tm, Formatter f) throws Exception {
        f.format("%-12s%d\n", "Id:", tm.getId());
        f.format("%-12s%s\n", "Type:", tm.getType());
        if (tm instanceof TM3BilingualTm) {
            f.format("%-12s%s\n", "Source:", 
                     ((TM3BilingualTm)tm).getSrcLocale());
            f.format("%-12s%s\n", "Target:", 
                    ((TM3BilingualTm)tm).getTgtLocale());
        }
        if (tm instanceof TM3SharedTm) {
            f.format("%-12s%d\n", "Storage Id:", 
                    ((TM3SharedTm)tm).getSharedStorageId());
        }
        Set<TM3Attribute> attrs = tm.getAttributes();
        if (attrs.size() > 0) {
            f.format("%s", "Attributes: ");
            for (TM3Attribute attr : attrs) {
                f.format("%s ", attr.getName());
            }
            f.format("\n");
        }
    }
}
