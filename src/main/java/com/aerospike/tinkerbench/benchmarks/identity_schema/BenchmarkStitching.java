package com.aerospike.tinkerbench.benchmarks.identity_schema;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.T;
import org.openjdk.jmh.annotations.*;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 1)
public class BenchmarkStitching extends BenchmarkIdentitySchema {

    private static final List<String> DEVICE_TYPES = List.of("Mobile", "Desktop", "Tablet", "SmartTV", "SmartWatch", "SmartSpeaker");

    private static final int DEVICE_CREATE_COUNT = 5;
    private static final Long DEVICE_ID_MAX = 1000L;
    private static final int IP_ADDR_CREATE_COUNT = 5;


    static class Device {
        final String type;
        final Long deviceId;

        public Device(final String type, final Long deviceId) {
            this.type = type;
            this.deviceId = deviceId;
        }
    }

    static class IpAddress {
        final String ipAddr;

        public IpAddress(final String ipAddr) {
            this.ipAddr = ipAddr;
        }
    }

    private static List<Device> createRandomDevices(int count) {
        final List<Device> devices = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            if (RANDOM.nextBoolean()) {
                final String device = DEVICE_TYPES.get(RANDOM.nextInt(DEVICE_TYPES.size()));
                final Long deviceId = RANDOM.nextLong(DEVICE_ID_MAX);
                devices.add(new Device(device, deviceId));
            }
        }
        return devices;
    }

    private static List<IpAddress> createRandomIpAddrs(int count) {
        final List<IpAddress> ipAddresses = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            if (RANDOM.nextBoolean()) {
                // Limit to 65536 different IP addresses for now.
                final String ipAddress = String.format("%d.%d.%d.%d",
                        192,
                        0,
                        RANDOM.nextInt(256),
                        RANDOM.nextInt(256));
                ipAddresses.add(new IpAddress(ipAddress));
            }
        }
        return ipAddresses;
    }


    class PartnerDataRow {
        final String partnerName;
        final String partnerIndividualId;
        final Object partnerEmail;
        final Object partnerPhone;
        final List<Device> devices;
        final List<IpAddress> ipAddrs;

        public PartnerDataRow(final String partnerName,
                              final String partnerIndividualId,
                              final Object partnerEmail,
                              final Object partnerPhone) {
            this.partnerName = partnerName;
            this.partnerIndividualId = partnerIndividualId;
            this.partnerEmail = partnerEmail;
            this.partnerPhone = partnerPhone;
            if (partnerEmail != null && partnerPhone != null) {
                this.devices = createRandomDevices(RANDOM.nextInt(1));
                this.ipAddrs = createRandomIpAddrs(RANDOM.nextInt(1));
            } else {
                this.devices = createRandomDevices(RANDOM.nextInt(DEVICE_CREATE_COUNT));
                this.ipAddrs = createRandomIpAddrs(RANDOM.nextInt(IP_ADDR_CREATE_COUNT));
            }
        }
    }

    private PartnerDataRow generatePartnerData() {
        if (RANDOM.nextBoolean()) {
            return new PartnerDataRow(getRandomPartnerName(), UUID.randomUUID().toString(), getEmail(), null);
        } else {
            return new PartnerDataRow(getRandomPartnerName(), UUID.randomUUID().toString(), null, getPhoneNumber());
        }
    }

    private PartnerDataRow generateCrossStitchPartnerData() {
        return new PartnerDataRow(getRandomPartnerName(), UUID.randomUUID().toString(), getEmail(), getPhoneNumber());
    }

    @Benchmark
    public void aPartnerCrossStitchApproaches() {
        // Start with an email and phone number on the partner. This will match to 2 golden entities email or phone number.
        // Sindex used to make this lookup fast.
        //
        // From here we add all the partner data (ip addr's and devices, if they do not already exist).
        // We then attach the partner identity to the golden entity.
        // We then attach the devices and ip addr's to the golden entity.
        // We then attach the devices and ip addr's to the partner.
        //
        // Finally, we execute the traversal.

        // Generate partner data.
        final PartnerDataRow partnerData = generateCrossStitchPartnerData();
        final GraphTraversal traversal;

        // Start traversal with GoldenEntity labelled as 'golden'
        traversal = g.V().has(GOLDEN_ENTITY_LABEL, EMAIL_PROPERTY_KEY, partnerData.partnerEmail).as("golden_email");
        traversal.V().has(GOLDEN_ENTITY_LABEL, PHONE_NUMBER_PROPERTY_KEY, partnerData.partnerPhone).as("golden_phone");

        // Loop through devices, keeping track of how many we add.
        int deviceCount = 0;
        for (final Device device : partnerData.devices) {
            // If Device with deviceId exists, use it, otherwise create it, label as 'device<cnt>'
            traversal.coalesce(
                    __.V(device.deviceId),
                    __.addV(DEVICE_LABEL).property("type", device.type).property(T.id, device.deviceId)).as("device" + deviceCount++);
        }
        int ipAddrCount = 0;
        for (IpAddress ipAddr : partnerData.ipAddrs) {
            // If IpAddress with ip_address exists, use it, otherwise create it, label as 'ipAddr<cnt>'
            traversal.coalesce(
                    __.V(ipAddr.ipAddr),
                    __.addV(IP_ADDRESS_LABEL).property(T.id, ipAddr.ipAddr)).as("ipAddr" + ipAddrCount++);
        }

        // Add PartnerIdentity, label as 'partner'.
        traversal.addV(PARTNER_IDENTITY_LABEL).as("partner");

        // Add edge from GoldenEntity to PartnerIdentity.
        traversal.addE("HAS_IDENTITY").from("golden_email").to("partner");
        traversal.addE("HAS_IDENTITY").from("golden_phone").to("partner");

        // Add edges to all Device vertices.
        for (int i = 0; i < deviceCount; i++) {
            // Add edge from GoldenEntity to Device.
            traversal.addE("HAS_DEVICE").from("golden_email").to("device" + i);
            traversal.addE("HAS_DEVICE").from("golden_phone").to("device" + i);

            // Add edge from PartnerIdentity to Device.
            traversal.addE("PROVIDED_DEVICE").from("partner").to("device" + i);
        }

        // Add edges to all IpAddress vertices.
        for (int i = 0; i < ipAddrCount; i++) {
            // Add edge from GoldenEntity to IpAddress.
            traversal.addE("HAS_IP_ADDR").from("golden_email").to("ipAddr" + i);
            traversal.addE("HAS_IP_ADDR").from("golden_phone").to("ipAddr" + i);

            // Add edge from PartnerIdentity to IpAddress.
            traversal.addE("PROVIDED_IP_ADDR").from("partner").to("ipAddr" + i);
        }
        traversal.iterate();
    }

    @Benchmark
    public void aPartnerApproaches() {
        // Start with an email or phone number on the partner. This will match to a golden entities email or phone number.
        // Sindex used to make this lookup fast.
        //
        // From here we add all the partner data (ip addr's and devices, if they do not already exist).
        // We then attach the partner identity to the golden entity.
        // We then attach the devices and ip addr's to the golden entity.
        // We then attach the devices and ip addr's to the partner.
        //
        // Finally, we execute the traversal.

        // Generate partner data.
        final PartnerDataRow partnerData = generatePartnerData();
        final GraphTraversal traversal;

        // Start traversal with GoldenEntity labelled as 'golden'
        if (partnerData.partnerEmail != null) {
            traversal = g.V().has(GOLDEN_ENTITY_LABEL, EMAIL_PROPERTY_KEY, partnerData.partnerEmail).as("golden");
        } else if (partnerData.partnerPhone != null) {
            traversal = g.V().has(GOLDEN_ENTITY_LABEL, PHONE_NUMBER_PROPERTY_KEY, partnerData.partnerPhone).as("golden");
        } else {
            throw new IllegalStateException("Partner must have either email or phone number.");
        }

        // Loop through devices, keeping track of how many we add.
        int deviceCount = 0;
        for (final Device device : partnerData.devices) {
            // If Device with deviceId exists, use it, otherwise create it, label as 'device<cnt>'
            traversal.coalesce(
                    __.V(device.deviceId),
                    __.addV(DEVICE_LABEL).property("type", device.type).property(T.id, device.deviceId)).as("device" + deviceCount++);
        }
        int ipAddrCount = 0;
        for (IpAddress ipAddr : partnerData.ipAddrs) {
            // If IpAddress with ip_address exists, use it, otherwise create it, label as 'ipAddr<cnt>'
            traversal.coalesce(
                    __.V(ipAddr.ipAddr),
                    __.addV(IP_ADDRESS_LABEL).property(T.id, ipAddr.ipAddr)).as("ipAddr" + ipAddrCount++);
        }

        // Add PartnerIdentity, label as 'partner'.
        traversal.addV(PARTNER_IDENTITY_LABEL).as("partner");

        // Add edge from GoldenEntity to PartnerIdentity.
        traversal.addE("HAS_IDENTITY").from("golden").to("partner");

        // Add edges to all Device vertices.
        for (int i = 0; i < deviceCount; i++) {
            // Add edge from GoldenEntity to Device.
            traversal.addE("HAS_DEVICE").from("golden").to("device" + i);

            // Add edge from PartnerIdentity to Device.
            traversal.addE("PROVIDED_DEVICE").from("partner").to("device" + i);
        }

        // Add edges to all IpAddress vertices.
        for (int i = 0; i < ipAddrCount; i++) {
            // Add edge from GoldenEntity to IpAddress.
            traversal.addE("HAS_IP_ADDR").from("golden").to("ipAddr" + i);

            // Add edge from PartnerIdentity to IpAddress.
            traversal.addE("PROVIDED_IP_ADDR").from("partner").to("ipAddr" + i);
        }
        traversal.iterate();
    }
}
