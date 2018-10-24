package test;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import util.GlobalLogger;

class GlobalLoggerTest {

	@Test
	void test() {
		GlobalLogger.getLogger().info("how are u");
		GlobalLogger.getLogger().info("how are u");
	}

}
