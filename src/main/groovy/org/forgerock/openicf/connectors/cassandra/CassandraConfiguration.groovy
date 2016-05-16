/*
 * DO NOT REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Copyright (c) 2016 ForgeRock AS. All rights reserved.
 *
 * The contents of this file are subject to the terms
 * of the Common Development and Distribution License
 * (the License). You may not use this file except in
 * compliance with the License.
 *
 * You can obtain a copy of the License at
 * http://opensource.org/licenses/CDDL-1.0
 * See the License for the specific language governing
 * permission and limitations under the License.
 *
 * When distributing Covered Code, include this CDDL
 * Header Notice in each file and include the License file
 * at http://opensource.org/licenses/CDDL-1.0
 * If applicable, add the following below the CDDL Header,
 * with the fields enclosed by brackets [] replaced by
 * your own identifying information:
 * "Portions Copyrighted [year] [name of copyright owner]"
 */
package org.forgerock.openicf.connectors.cassandra

import com.datastax.driver.core.Cluster
import org.codehaus.groovy.runtime.InvokerHelper
import org.forgerock.openicf.misc.scriptedcommon.ScriptedConfiguration
import org.identityconnectors.common.Assertions
import org.identityconnectors.common.StringUtil
import org.identityconnectors.common.security.GuardedString
import org.identityconnectors.framework.spi.AbstractConfiguration
import org.identityconnectors.framework.spi.ConfigurationClass
import org.identityconnectors.framework.spi.ConfigurationProperty

/**
 * Extends the {@link AbstractConfiguration} class to provide all the necessary
 * parameters to initialize the Cassandra Connector.
 *
 */
@ConfigurationClass(skipUnsupported = true)
public class CassandraConfiguration extends ScriptedConfiguration {

    // Exposed configuration properties.

    /**
     * The connector to connect to.
     */
    private String[] contactPoints;

    /**
     * The Remote user to authenticate with.
     */
    private String keySpace = null;

    /**
     * The Remote user to authenticate with.
     */
    private String username = null;

    /**
     * The Password to authenticate with.
     */
    private GuardedString password = null;


    Closure initClosure = null;

    /**
     * Constructor.
     */
    public CassandraConfiguration() {

    }


    @ConfigurationProperty(order = 1, displayMessageKey = "contactPoints.display",
            groupMessageKey = "basic.group", helpMessageKey = "contactPoints.help",
            required = true, confidential = false)
    String[] getContactPoints() {
        return contactPoints
    }

    void setContactPoints(String[] contactPoints) {
        this.contactPoints = contactPoints
    }

    @ConfigurationProperty(order = 2, displayMessageKey = "keySpace.display",
            groupMessageKey = "basic.group", helpMessageKey = "keySpace.help",
            required = true, confidential = false)
    String getKeySpace() {
        return keySpace
    }

    void setKeySpace(String keySpace) {
        this.keySpace = keySpace
    }

    @ConfigurationProperty(order = 3, displayMessageKey = "username.display",
            groupMessageKey = "basic.group", helpMessageKey = "username.help",
            required = true, confidential = false)
    public String getUsername() {
        return username;
    }

    public void setUsername(String remoteUser) {
        this.username = remoteUser;
    }

    @ConfigurationProperty(order = 4, displayMessageKey = "password.display",
            groupMessageKey = "basic.group", helpMessageKey = "password.help",
            confidential = true)
    public GuardedString getPassword() {
        return password;
    }

    public void setPassword(GuardedString password) {
        this.password = password;
    }

    /**
     * {@inheritDoc}
     */
    public void validate() {
        if (getContactPoints() == null || getContactPoints().size() < 1 ||
                getContactPoints().any({ cp -> StringUtil.isBlank(cp) })) {
            throw new IllegalArgumentException("ContactPoints cannot be null or empty.");
        }

        Assertions.blankCheck(getUsername(), "remoteUser");

        Assertions.nullCheck(getPassword(), "password");
    }


    protected Script createCustomizerScript(Class customizerClass, Binding binding) {

        customizerClass.metaClass.customize << { Closure cl ->
            initClosure = null
            releaseClosure = null

            def delegate = [
                    init   : { Closure c ->
                        initClosure = c
                    },
                    release: { Closure c ->
                        setReleaseClosure(c)
                    }
            ]
            cl.setDelegate(new Reference(delegate));
            cl.setResolveStrategy(Closure.DELEGATE_FIRST);
            cl.call();
        }

        return InvokerHelper.createScript(customizerClass, binding);
    }

    private Cluster cluster = null;

    Cluster getCluster() {
        if (null == cluster) {
            synchronized (this) {
                if (null == cluster) {
                    getGroovyScriptEngine()
                    Closure clone = initClosure.rehydrate(this, this, this);
                    clone.setResolveStrategy(Closure.DELEGATE_FIRST);
                    Cluster.Builder builder = Cluster.builder()
                    clone(builder)
                    cluster = builder.build();
                }
            }
        }
        return cluster;
    }

    @Override
    void release() {
        synchronized (this) {
            super.release()
            if (null != cluster) {
                cluster.close();
                cluster = null;
            }
        }
    }
}
