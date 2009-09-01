/* 
 * Copyright (c) 2007-2008, Hazel Ltd. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at 
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.hazelcast.web;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ServletBase extends HttpServlet {
    class Config implements ServletConfig {

        ServletConfig original = null;

        ServletContext app = null;

        final Set<String> paramNames = new HashSet<String>();

        public Config(final ServletConfig original) {
            super();
            this.original = original;
            final Enumeration<String> names = original.getInitParameterNames();
            while (names.hasMoreElements()) {
                final String name = names.nextElement();
                if (!name.startsWith("*hazelcast")) {
                    paramNames.add(name);
                }
            }
        }

        public String getInitParameter(final String arg0) {
            return original.getInitParameter(arg0);
        }

        public Enumeration getInitParameterNames() {
            final Iterator<String> it = paramNames.iterator();
            return new Enumeration<String>() {

                public boolean hasMoreElements() {
                    return it.hasNext();
                }

                public String nextElement() {
                    return it.next();
                }
            };
        }

        public ServletContext getServletContext() {
            return getCurrentContext();
        }

        public String getServletName() {
            return original.getServletName();
        }

        private ServletContext getCurrentContext() {
            if (app == null) {
                app = WebFilter.getServletContext(original.getServletContext());
                if (app != null) {
                    return app;
                }
                return original.getServletContext();
            }
            return app;
        }

    }

    protected static Logger logger = Logger.getLogger(ServletBase.class.getName());

    private static final boolean DEBUG = true;

    @Override
    public void init(final ServletConfig servletConfig) throws ServletException {
        WebFilter.ensureServletContext(servletConfig.getServletContext());
        super.init(servletConfig);
    }

    protected void debug(final Object obj) {
        if (DEBUG) {
            logger.log(Level.INFO, obj.toString());
        }
    }

}
