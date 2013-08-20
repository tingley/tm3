package com.globalsight.ling.tm3.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Representation of a single TU/segment.
 * <p>
 * Note that all changes to a TU, its TUVs, and any of its 
 * dependent data structures are considered transient and are not
 * persisted unless a subsequent call to {@link TM3Tm#modifyTu(TM3Tu, TM3Event)}
 * is made.  
 */
public class TM3Tu<T extends TM3Data> {
    
    private Long id;
    private TM3Tuv<T> sourceTuv;
    private List<TM3Tuv<T>> targetTuvs = new ArrayList<TM3Tuv<T>>();
    private TM3Tm<T> tm;
    private TuStorage<T> storage;
    private Map<TM3Attribute, Object> attributes = 
                    new HashMap<TM3Attribute, Object>();
   
    TM3Tu() {   
    }
   
    public Long getId() {
        return id;
    }
    
    void setId(Long id) {
        this.id = id;
    }
    
    public TM3Tu(TM3Tm<T> tm, TuStorage<T> storage, TM3Tuv<T> sourceTuv,
            Map<TM3Attribute, Object> attributes) {
        this.tm = tm;
        this.storage = storage;
        this.sourceTuv = sourceTuv;
        for (Map.Entry<TM3Attribute, Object> e : attributes.entrySet()) {
            e.getKey().getValueType().checkValue(
                    e.getValue(), e.getKey().getName());
        }
        this.attributes = attributes;
        sourceTuv.setTu(this);
    }
    
    // Storage layer ctor
    TM3Tu(TM3Tm<T> tm, TuStorage<T> storage, TM3Tuv<T> sourceTuv,
            List<TM3Tuv<T>> targetTuvs, Map<TM3Attribute, Object> attributes) {
        this.tm = tm;
        this.storage = storage;
        this.sourceTuv = sourceTuv; 
        this.targetTuvs = targetTuvs;
        this.attributes = attributes;
        sourceTuv.setTu(this);
    }

    public TM3Tuv<T> getSourceTuv() {
        return sourceTuv;
    }

    void setSourceTuv(TM3Tuv<T> sourceTuv) {
        this.sourceTuv = sourceTuv;
        sourceTuv.setTu(this);
    }

    /**
     * Get all target TUVs contained in this TU.
     * @return set of TUVs
     */
    public List<TM3Tuv<T>> getTargetTuvs() {
        return targetTuvs;
    }

    /**
     * Return a TUV for the specified locale.
     * @param locale desired locale
     * @return TUV, or null if no TUV exists for the locale
     */
    public List<TM3Tuv<T>> getLocaleTuvs(TM3Locale locale) {
        if (sourceTuv.getLocale().equals(locale)) {
            return Collections.singletonList(sourceTuv);
        }
        List<TM3Tuv<T>> localeTuvs = new ArrayList<TM3Tuv<T>>();
        for (TM3Tuv<T> tuv : targetTuvs) {
            if (tuv.getLocale().equals(locale)) {
                localeTuvs.add(tuv);
            }
        }
        return localeTuvs;
    }
    
    /**
     * Return all TUV in this TU, including both source and
     * targets.
     */
    public List<TM3Tuv<T>> getAllTuv() {
        List<TM3Tuv<T>> allTuv = new ArrayList<TM3Tuv<T>>();
        allTuv.add(getSourceTuv());
        allTuv.addAll(getTargetTuvs());
        return allTuv;
    }
    
    /**
     * Remove and return all TUV for the specified locale.
     * @param locale
     */
    public void removeTargetTuvByLocale(TM3Locale locale) {
        for (Iterator<TM3Tuv<T>> it = targetTuvs.iterator(); it.hasNext(); ) {
            TM3Tuv<T> tgt = it.next();
            if (tgt.getLocale().equals(locale)) {
                it.remove();
            }
        }
    }
    
    /**
     * Remove a specific TUV.
     * @param locale locale of the TUV to remove
     * @param content content of the TUV to remove
     * @return true if the the TUV was found and removed, false
     *        if it the specified TUV does not belong to this TU
     */
    public boolean removeTargetTuv(TM3Locale locale, T content) {
        Iterator<TM3Tuv<T>> it = targetTuvs.iterator();
        while (it.hasNext()) {
            TM3Tuv<T> tgt = it.next();
            if (tgt.getLocale().equals(locale) && 
                tgt.getContent().equals(content)) {
                it.remove();
                return true;
            }
        }
        return false;
    }
    
    /**
     * Remove all target TUV.
     */
    public void removeTargetTuvs() {
        this.targetTuvs.clear();
    }
    
    TuStorage<T> getStorage() {
        return storage;
    }
    
    void setStorage(TuStorage<T> storage) {
        this.storage = storage;
    }

    TM3Tm<T> getTm() {
        return tm;
    }
    
    void setTm(TM3Tm<T> tm) {
        this.tm = tm;
    }

    /**
     * Add a new target TUV, unless an identical TUV already exists.
     * @param locale
     * @param content
     * @param event
     * @return Returns the new TUV, or null if an identical TUV already exists.
     */
    public TM3Tuv<T> addTargetTuv(TM3Locale locale, T content, TM3Event event) {
        if (event == null) {
            throw new IllegalArgumentException("event can not be null");
        }
        // Don't save identical (in both locale and content) targets
        for (TM3Tuv<T> tuv : targetTuvs) {
            if (tuv.getLocale().equals(locale) && tuv.getContent().equals(content)) {
                return null;
            }
        }
        TM3Tuv<T> tuv = new TM3Tuv<T>(locale, content, event); 
        targetTuvs.add(tuv);
        tuv.setTu(this);
        return tuv;
    }

    public TM3EventLog getHistory() {
        throw new UnsupportedOperationException(); // TODO
    }

    /**
     * Return attributes for this TU.  Only attributes for which a value is set
     * will be returned.
     */
    public Map<TM3Attribute, Object> getAttributes() {
        return attributes;
    }

    /**
     * Return the value for a single attribute.
     * @param attribute
     */
    public Object getAttribute(TM3Attribute attribute) {
        return attributes.get(attribute);
    }
    
    /**
     * Set the value for a given attribute on this TU.
     * @param attribute
     * @param value new attribute value.  A value of null will remove the
     *        attribute from this TU.
     */
    public void setAttribute(TM3Attribute attribute, Object value) {
        attribute.getValueType().checkValue(value, attribute.getName());
        attributes.put(attribute, value);
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public boolean equals(Object o) {
        if (o == null || !(o instanceof TM3Tu)) {
            return false;
        }
        TM3Tu<T> tu = (TM3Tu<T>)o;
        if (getId() == null && tu.getId() == null) {
            return this == tu;
        }
        if (getId() != null && tu.getId() != null) {
            return getId().equals(tu.getId());
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        if (getId() == null) {
            return System.identityHashCode(this);
        }
        return getId().hashCode();
    }
}
