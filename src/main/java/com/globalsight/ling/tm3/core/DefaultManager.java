package com.globalsight.ling.tm3.core;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.jdbc.ReturningWork;

/**
 * Top-level interface for interacting with TM3.
 * <p>
 * DefaultManager instances are <b>not thread safe</b>.
 */
public class DefaultManager<T extends TM3Data> implements TM3Manager<T> {
    
    private Session session;
    
    private DefaultManager(Session session) {
        this.session = session;
    }
        
    public static <T extends TM3Data> TM3Manager<T> create(Session session) {
        return new DefaultManager<T>(session);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<TM3Tm<T>> getAllTms( 
            TM3DataFactory<T> factory) throws TM3Exception {
        try {
            List<? extends TM3Tm<T>> tms = session.createCriteria(BaseTm.class).list();
            for (TM3Tm<T> t : tms) {
                BaseTm<T> tm = (BaseTm<T>)t;
                injectTm(tm, factory);
            }
            return (List<TM3Tm<T>>) tms;
        }
        catch (HibernateException e) {
            throw new TM3Exception(e);
        }
    }

    
    @SuppressWarnings("unchecked")
    public TM3Tm<T> getTm( 
            TM3DataFactory<T> factory, long id) throws TM3Exception {
        try {
            BaseTm<T> tm = (BaseTm<T>)session.get(BaseTm.class, id);
            if (tm != null) {
                injectTm(tm, factory);
            }
            return tm;
        }
        catch (HibernateException e) {
            throw new TM3Exception(e);
        }
    }

    public TM3BilingualTm<T> getBilingualTm(
            TM3DataFactory<T> factory, long id) 
            throws TM3Exception {
        TM3Tm<T> tm = getTm(factory, id);
        if (tm == null) {
            return null;
        }
        if (!(tm instanceof TM3BilingualTm)) {
            return null;
        }
        return (TM3BilingualTm<T>)tm;
    }
    
    
    /**
     * Create a new bilingual tm.
     * 
     * @param session
     * @param factory
     * @param srcLocale
     * @param tgtLocale
     * @throws TM3Exception
     */
    public TM3BilingualTm<T> createBilingualTm(
            TM3DataFactory<T> factory, 
            Set<TM3Attribute> inlineAttributes,
            TM3Locale srcLocale, TM3Locale tgtLocale) 
            throws TM3Exception {
        
        if (srcLocale == null) {
            throw new IllegalArgumentException("Invalid source locale");
        }
        if (tgtLocale == null) {
            throw new IllegalArgumentException("Invalid target locale");
        }
        
        try {
            return init(new BilingualTm<T>(factory, srcLocale, tgtLocale),
                        inlineAttributes);
        }
        catch (SQLException e) {
            throw new TM3Exception(e);
        }
        catch (HibernateException e) {
            throw new TM3Exception(e);
        }
    }
    
    public TM3Tm<T> createMultilingualTm(
            TM3DataFactory<T> factory, Set<TM3Attribute> inlineAttributes)
            throws TM3Exception {
        
        try {
            return init(new MultilingualTm<T>(factory), inlineAttributes);
        }
        catch (SQLException e) {
            throw new TM3Exception(e);
        }
        catch (HibernateException e) {
            throw new TM3Exception(e);
        }
    }

    public TM3SharedTm<T> createMultilingualSharedTm(
            TM3DataFactory<T> factory,
            Set<TM3Attribute> inlineAttributes, long sharedStorageId) 
            throws TM3Exception {
    
        try {
            return init(new MultilingualSharedTm<T>(sharedStorageId, factory),
                        inlineAttributes);
        }
        catch (SQLException e) {
            throw new TM3Exception(e);
        }
        catch (HibernateException e) {
            throw new TM3Exception(e);
        }
    }
    
    /**
     * Remove a tm.
     * @param session
     * @param tm
     * @throws TM3Exception
     */
    public void removeTm(TM3Tm<T> tm) 
                        throws TM3Exception {
        try {
            StorageInfo<T> storage = ((BaseTm<T>)tm).getStorageInfo();
            storage.destroy();
            session.delete(tm);
        }
        catch (SQLException e) {
            throw new TM3Exception(e);
        }
    }
    
    private <K extends BaseTm<T>> K init(K tm, 
            Set<TM3Attribute> inlineAttributes)
            throws SQLException, HibernateException {
        tm.setManager(this);
        tm.setSession(session);
        session.persist(tm);
        for (TM3Attribute attr : inlineAttributes) {
            attr.setTm(tm);
            tm.addAttribute(attr);
        }
        session.flush(); // Sync the object to get an ID
        tm.getStorageInfo().create();
        return tm;
    }
    
    private <V extends BaseTm<T>> V injectTm(
                    V tm, TM3DataFactory<T> factory) {
        tm.setDataFactory(factory);
        tm.setSession(session);
        tm.setManager(this);
        return tm;
    }

    @Override
    public boolean createStoragePool(final long id,
            final Set<TM3Attribute> inlineAttributes) throws TM3Exception {
        return session.doReturningWork(new ReturningWork<Boolean>() {
            @Override
            public Boolean execute(Connection conn) throws SQLException {
                try {
                   return new SharedStorageTables(conn, id).create(inlineAttributes);
                }
                catch (SQLException e) {
                    throw new TM3Exception(e);
                }
            }
        });
    }

    @Override
    public boolean removeStoragePool(final long id) throws TM3Exception {
        return session.doReturningWork(new ReturningWork<Boolean>() {
            @Override
            public Boolean execute(Connection conn) throws SQLException {
                try {
                   return new SharedStorageTables(conn, id).destroy();
                }
                catch (SQLException e) {
                    throw new TM3Exception(e);
                }
            }
        });
    }

}
