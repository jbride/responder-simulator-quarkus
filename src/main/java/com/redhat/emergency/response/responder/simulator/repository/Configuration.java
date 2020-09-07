package com.redhat.emergency.response.responder.simulator.repository;

import org.infinispan.commons.configuration.BasicConfiguration;

public class Configuration implements BasicConfiguration {

    private String name;

    private String mode;

    private Integer owners;

    private static final String CACHE_CONFIG = "<infinispan><cache-container>" +
            "<distributed-cache name=\"%s\"></distributed-cache>" +
            "</cache-container></infinispan>";

    @Override
    public String toXMLString(String name) {
        return "<infinispan><cache-container>"
                + "<distributed-cache name=\"" + name + "\" "
                + "mode=\"" + mode + "\" "
                + "owners=\"" + owners + "\">"
                + "</distributed-cache>"
                + "</cache-container></infinispan>";
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private final Configuration configuration = new Configuration();

        public Builder() {}

        public Builder name(String name) {
            configuration.name = name;
            return this;
        }

        public Builder mode(String mode) {
            configuration.mode = mode.toUpperCase();
            return this;
        }

        public Builder owners(Integer owners) {
            configuration.owners = owners;
            return this;
        }

        public Configuration build() {
            return configuration;
        }

    }
}
