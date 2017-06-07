package io.valadan.consul;

import static com.pszymczyk.consul.ConsulStarterBuilder.consulStarter;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import com.pszymczyk.consul.ConsulProcess;

public abstract class BaseIntegrationTest {

    protected static ConsulProcess consul;

    @BeforeClass
    public static void beforeClass() {
        consul = consulStarter().withHttpPort(8500).build().start();
    }

    @AfterClass
    public static void afterClass() {
        consul.close();
    }

    @Before
    public void beforeTest() {
        consul.reset();
    }
}