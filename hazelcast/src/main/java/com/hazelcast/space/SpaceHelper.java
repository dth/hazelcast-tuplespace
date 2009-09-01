package com.hazelcast.space;

import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Field;

/**
 * Created by IntelliJ IDEA.
 * User: dth
 * Date: 23/08/2009
 * Time: 7:51:58 PM
 * To change this template use File | Settings | File Templates.
 */
public class SpaceHelper {

    public static Map extractElements(Object o) {
        Map<String, String> m = extractMethods(o);
        m.putAll(extractFields(o));
        return m;
    }

    public static boolean elementsMatch(Map templateElements, Map entryElements) {
        Set templateSet = templateElements.entrySet();
        Set entrySet = entryElements.entrySet();
        return entrySet.containsAll(templateSet);
    }


    /**
     * Extract public getXxx'ers
     */
    private static Map<String, String> extractMethods(Object o) {
        Method[] methods = o.getClass().getMethods();
        Map<String, String> map = new HashMap<String, String>();
        for (Method m : methods) {
            String name = m.getName();
            if (name.startsWith("get") && m.getTypeParameters().length == 0) {
                try {
                    Object v = m.invoke(o, null);
                    if (v != null) {
                        // TODO: remove "class" from getClass output
                        map.put(name, String.valueOf(v));
                    }
                } catch (IllegalAccessException e) {
                    // TODO
                } catch (InvocationTargetException e) {
                    // TODO
                }
            }
        }
        return map;
    }

    /**
     * Extract public fields from the object.
     */
    private static Map<String, String> extractFields(Object o) {
        Field[] fields = o.getClass().getFields();
        Map<String, String> m = new HashMap<String, String>();
        for (Field f : fields) {
            try {
                String name = f.getName();
                Object value = f.get(o);
                if (value != null) {
                    m.put(name, String.valueOf(value));
                }
            } catch (IllegalAccessException e) {
                // TODO
            }
        }
        return m;
    }

}
