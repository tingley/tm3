package com.globalsight.ling.tm3.core;

import java.sql.SQLException;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

/**
 * A handle to the entire TM.
 */
class AllTusDataHandle<T extends TM3Data> extends AbstractDataHandle<T> {

    AllTusDataHandle(BaseTm<T> tm) {
        super(tm);
    }
    
    AllTusDataHandle(BaseTm<T> tm, Date start, Date end) {
        super(tm, start, end);
    }

    @Override
    public long getCount() throws TM3Exception {
        try {
            return getTm().getStorageInfo().getTuStorage()
                .getTuCount(getStart(), getEnd());
        } catch (SQLException e) {
            throw new TM3Exception(e);
        }
    }

    @Override
    public long getTuvCount() throws TM3Exception {
        try {
            return getTm().getStorageInfo().getTuStorage()
                .getTuvCount(getStart(), getEnd());
        } catch (SQLException e) {
            throw new TM3Exception(e);
        }
    }

    @Override
    public void purgeData() throws TM3Exception {
        try {
            getTm().getStorageInfo().getTuStorage()
                .deleteTus(getStart(), getEnd());
        } catch (SQLException e) {
            throw new TM3Exception(e);
        }
    }
    
    @Override
    public Iterator<TM3Tu<T>> iterator() throws TM3Exception {
        return new AllTusIterator();
    }

    class AllTusIterator extends AbstractDataHandle<T>.TuIterator {
        @Override
        protected void loadPage() {
            try {
                // Load 100 at a time
                List<TM3Tu<T>> page = getTm().getStorageInfo().getTuStorage()
                            .getTuPage(startId, 100, getStart(), getEnd());
                if (page.size() > 0) {
                    startId = page.get(page.size() - 1).getId();
                    currentPage = page.iterator();
                }
            }
            catch (SQLException e) {
                throw new TM3Exception(e);
            }
        }
    }
}
