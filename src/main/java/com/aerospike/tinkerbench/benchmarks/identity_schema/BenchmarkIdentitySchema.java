package com.aerospike.tinkerbench.benchmarks.identity_schema;

import com.aerospike.tinkerbench.benchmarks.TinkerBench;
import com.aerospike.tinkerbench.util.BenchmarkUtil;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.infra.BenchmarkParams;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public abstract class BenchmarkIdentitySchema extends TinkerBench {
    protected static final String DEVICE_LABEL = "Device";
    protected static final String IP_ADDRESS_LABEL = "IpAddress";
    protected static final String ACCOUNT_LABEL = "Account";
    protected static final String HOUSEHOLD_LABEL = "Household";
    protected static final String COOKIE_LABEL = "Cookie";

    protected static final String PHONE_NUMBER_PROPERTY_KEY = "phone_number_hash";
    protected static final String EMAIL_PROPERTY_KEY = "email_hash";

    protected static final String PARTNER_IDENTITY_LABEL = "Partner";
    protected static final String GOLDEN_ENTITY_LABEL = "GoldenEntity";

    protected static final Set<String> SIGNAL_LABELS = Set.of(
            DEVICE_LABEL,
            IP_ADDRESS_LABEL,
            ACCOUNT_LABEL,
            HOUSEHOLD_LABEL,
            COOKIE_LABEL);
    protected static final Set<String> PROPERTY_KEYS = Set.of(PHONE_NUMBER_PROPERTY_KEY, EMAIL_PROPERTY_KEY);
    protected static final Set<String> ENTITY_IDENTITY_LABELS = Set.of(
            PARTNER_IDENTITY_LABEL,
            GOLDEN_ENTITY_LABEL);
    protected final Map<String, List<Object>> signalToID = new HashMap<>();
    protected final Map<String, List<Object>> entityIdentityToId = new HashMap<>();
    protected static final Random RANDOM = new Random();
    protected static final List<String> PARTNER_NAMES = List.of("TDID", "Tapad", "UID2", "Lotame", "RampID", "ID5", "Epsilon", "Qualtrics", "DataAxle", "Moneris", "CriticalMention", "Clutch", "Datarade", "Adobe", "Tableau", "CleverTab", "Marketo");

    protected static String getRandomPartnerName() {
        // TODO: Update Partner names
        return PARTNER_NAMES.get(RANDOM.nextInt(PARTNER_NAMES.size()));
    }

    @Setup
    public void setupIdentitySchemaMappings(final BenchmarkParams benchmarkParams) {
        System.out.println("Creating signal id mapping.");
        BenchmarkUtil.collectBenchmarkLabelIdMapping(getRandomGraphTraversalSource(), SIGNAL_LABELS, signalToID);
        System.out.println("Completed signal id mapping creation.");

        System.out.println("Creating property key mapping.");
        BenchmarkUtil.collectBenchmarkPropertyLabelMapping(getRandomGraphTraversalSource(), PROPERTY_KEYS, GOLDEN_ENTITY_LABEL, signalToID);
        System.out.println("completed property key mapping creation.");

        System.out.println("Creating entity and identity id mapping.");
        BenchmarkUtil.collectBenchmarkLabelIdMapping(getRandomGraphTraversalSource(), ENTITY_IDENTITY_LABELS, entityIdentityToId);
        System.out.println("Completed entity and identity mapping creation.");
    }

    protected Object getDeviceId() {
        final List<Object> deviceIds = new ArrayList<>(signalToID.get(DEVICE_LABEL));
        return deviceIds.get(RANDOM.nextInt(deviceIds.size()));
    }

    protected Object getIpAddress() {
        final List<Object> ipAddressIds = new ArrayList<>(signalToID.get(IP_ADDRESS_LABEL));
        return ipAddressIds.get(RANDOM.nextInt(ipAddressIds.size()));
    }

    protected Object getGoldenEntity() {
        final List<Object> goldenEntity = new ArrayList<>(entityIdentityToId.get(GOLDEN_ENTITY_LABEL));
        return goldenEntity.get(RANDOM.nextInt(goldenEntity.size()));
    }

    protected Object getPartnerIdentity() {
        final List<Object> partnerIdentity = new ArrayList<>(entityIdentityToId.get(PARTNER_IDENTITY_LABEL));
        return partnerIdentity.get(RANDOM.nextInt(partnerIdentity.size()));
    }

    protected Object getEmail() {
        final List<Object> emailIds = new ArrayList<>(signalToID.get(EMAIL_PROPERTY_KEY));
        return emailIds.get(RANDOM.nextInt(emailIds.size()));
    }

    protected Object getPhoneNumber() {
        final List<Object> phoneNumber = new ArrayList<>(signalToID.get(PHONE_NUMBER_PROPERTY_KEY));
        return phoneNumber.get(RANDOM.nextInt(phoneNumber.size()));
    }
}
