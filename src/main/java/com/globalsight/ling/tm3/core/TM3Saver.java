package com.globalsight.ling.tm3.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An object that allows complex save requests to be sent to the TM
 * in a somewhat orderly way.
 * <p>
 * A sample invocation might look like this:
 * <pre>
 *   TM3Saver<T> saver = tm.createSaver();
 *   for (<i>some condition</i>) {
 *      saver.tu(srcContent, srcLocale, event)
 *           .attr(attr1, value1)
 *           .attr(attr2, value2)
 *           .tuv(frenchContent, frenchLocale, event)
 *           .tuv(germanContent, germanLocale, event);
 *   }
 *   saver.save(TM3SaveMode.MERGE);
 * </pre>
 */
public abstract class TM3Saver<T extends TM3Data> {

    List<Tu> tus = new ArrayList<Tu>();
    
    TM3Saver() {
    }
    
    /**
     * Resets the saver object.
     */
    public void reset() {
        tus.clear();
    }
    
    /**
     * Add a new TU to this save operation, identified by its 
     * source TUV.  Additional calls may be made to the returned
     * Tu object to add targets TUV, attribute values, etc.
     * <p>
     * Note that nothing will be saved to the TM until a call to
     * {{@link #save(TM3SaveMode)} is made. 
     * @param content source content
     * @param locale source locale
     * @param event source tuv event
     */
    public Tu tu(T content, TM3Locale locale, TM3Event event) {
        Tu tu = new Tu(content, locale, event);
        tus.add(tu);
        return tu;
    }
    
    /**
     * Update the TM based on the contents of this saver.  This will
     * flush all TU and TUV to the database.
     * @param mode Save mode
     * @throws TM3Exception
     */
    public abstract List<TM3Tu<T>> save(TM3SaveMode mode) throws TM3Exception;
    
    /**
     * Representation of an unsaved TU, created by a call to 
     * {@link TM3Saver#tu(TM3Data, TM3Locale, TM3Event)}.  Method calls
     * on this object can add target TUV data or attributes.
     */
    public class Tu {
        Tuv srcTuv;
        List<Tuv> targets = new ArrayList<Tuv>(); 
        Map<TM3Attribute, Object> attrs = new HashMap<TM3Attribute, Object>();
        
        Tu(T content, TM3Locale locale, TM3Event event) {
            srcTuv = new Tuv(content, locale, event);
        }
        
        /**
         * Add a single attribute/value pair to this TU.
         * @param attr attribute
         * @param value value
         * @return this
         */
        public Tu attr(TM3Attribute attr, Object value) {
            attrs.put(attr, value);
            return this;
        }

        /**
         * Add multiple attribute/value pairs to this TU. 
         * @param pairs attribute/value pairs
         * @return this
         */
        public Tu attrs(Map<TM3Attribute, Object> pairs) {
            if (pairs != null) {
                attrs.putAll(pairs);
            }
            return this;
        }

        /**
         * Add a single target TUV to this TU.
         * @param content target content
         * @param locale target locale
         * @param event target TUV event
         * @return this
         */
        public Tu target(T content, TM3Locale locale, TM3Event event) {
            targets.add(new Tuv(content, locale, event));
            return this;
        }
        
        /**
         * Add multiple target TUVs to this TU.
         * @param t Map of locales to target data
         * @param event target TUV event
         * @return this
         */
        public Tu targets(Map<TM3Locale, T> t, TM3Event event) {
            for (Map.Entry<TM3Locale, T> e : t.entrySet()) {
                targets.add(new Tuv(e.getValue(), e.getKey(), event));
            }
            return this;
        }
        
        /**
         * Update the TM based on the contents of this saver.  This will
         * flush all TU and TUV to the database.  This method is provided
         * for convenience and is equivalent to calling 
         * {@link TM3Saver#save TM3Saver.save}.
         * @param mode Save mode
         * @throws TM3Exception
         */
        public List<TM3Tu<T>> save(TM3SaveMode mode) {
            return TM3Saver.this.save(mode);
        }
        
    }
    
    public class Tuv {
        T content;
        TM3Locale locale;
        TM3Event event;
        Tuv(T content, TM3Locale locale, TM3Event event) {
            this.content = content;
            this.locale = locale;
            this.event = event;
        }
        public T getContent() {
            return content;
        }
        public TM3Locale getLocale() {
            return locale;
        }
        public TM3Event getEvent() {
            return event;
        }
    }
}
