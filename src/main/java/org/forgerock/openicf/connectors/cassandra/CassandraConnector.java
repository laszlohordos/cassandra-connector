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

package org.forgerock.openicf.connectors.cassandra;

import com.datastax.driver.core.Cluster;
import groovy.lang.Binding;
import org.forgerock.openicf.misc.scriptedcommon.OperationType;
import org.forgerock.openicf.misc.scriptedcommon.ScriptedConnectorBase;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.OperationOptions;
import org.identityconnectors.framework.common.objects.Uid;
import org.identityconnectors.framework.spi.Connector;
import org.identityconnectors.framework.spi.ConnectorClass;

import java.util.Set;

/**
 * Main implementation of the Cassandra Connector.
 */
@ConnectorClass(
        displayNameKey = "Cassandra.connector.display",
        configurationClass = CassandraConfiguration.class, messageCatalogPaths = {
        "org/forgerock/openicf/connectors/groovy/Messages",
        "org/forgerock/openicf/connectors/cassandra/Messages"})
public class CassandraConnector extends ScriptedConnectorBase<CassandraConfiguration> implements Connector {

    /**
     * Setup logging for the {@link CassandraConnector}.
     */
    private static final Log logger = Log.getLog(CassandraConnector.class);

    protected Binding createBinding(Binding arguments, OperationType action, ObjectClass objectClass, Uid uid, Set<Attribute> attributes, OperationOptions options) {
        Binding b = super.createBinding(arguments, action, objectClass, uid, attributes, options);
        CassandraConfiguration cfg = ((CassandraConfiguration) this.getScriptedConfiguration());
        Cluster cluster = cfg.getCluster();
        if (null != cfg.getKeySpace()) {
            b.setVariable("session", cluster.connect(cfg.getKeySpace()));
        } else {
            b.setVariable("session", cluster.connect());
        }
        return b;
    }

}
