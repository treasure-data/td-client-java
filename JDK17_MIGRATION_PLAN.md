# JDK 8 to JDK 17 Migration Plan

## Overview
Implement the JDK 8 to JDK 17 migration for td-client-java, including replacement of OkHttp with pure JDK HTTP client. This is a breaking change requiring a major version bump from 1.2.0 to 2.0.0.

## Current State Analysis
- **Current version**: 1.2.0 (pom.xml:8)
- **Current JDK target**: 1.8 (pom.xml:58)
- **CircleCI**: Already tests JDK 8 and 17 (config.yml:46)
- **Guava Functions**: 4 deprecated methods exist as wrappers around java.util.function.Function
- **OkHttp Usage**: Currently uses OkHttp 3.14.9 for HTTP operations (pom.xml:72)
- **Dependencies**: Most appear JDK 17 compatible, but OkHttp will be replaced

## Implementation Strategy

### Phase 1: Build Configuration Updates
**Priority: Critical - Foundation for all other changes**

#### 1.1 Update Maven Configuration (pom.xml)
- **Line 58**: Change `<project.build.targetJdk>1.8</project.build.targetJdk>` to `17`
- **Line 8**: Bump version `<version>1.2.0</version>` to `<version>2.0.0</version>`
- **Lines 581-589**: Remove or update the `doclint-java8-disable` profile to JDK 17 equivalent
- **Verify**: All Maven plugins are JDK 17 compatible (they appear to be)

#### 1.2 Update CircleCI Configuration (.circleci/config.yml)
- **Line 46**: Change matrix from `[jdk8, jdk17]` to `[jdk17]` only
- **Lines 6-8**: Remove the jdk8 executor definition
- **Lines 28-30**: Remove JDK8-specific sleep logic if needed

### Phase 2: HTTP Client Migration - Replace OkHttp with JDK HTTP Client
**Priority: Critical - Major architectural change**

#### 2.1 Dependency Updates (pom.xml)
- **Lines 107-116**: Remove OkHttp dependencies (`okhttp` and `mockwebserver`)
- **Lines 300-302**: Remove OkHttp shading configuration from Maven Shade plugin
- **Line 72**: Remove `<okhttp.version>3.14.9</okhttp.version>` property
- Update shading configuration to remove `okhttp3` and `okio` relocations

#### 2.2 Core HTTP Client Replacement (TDHttpClient.java)
- **Lines 33-38**: Replace OkHttp imports with `java.net.http.*` imports:
  - `java.net.http.HttpClient`
  - `java.net.http.HttpRequest`
  - `java.net.http.HttpResponse`
  - `java.net.http.HttpTimeoutException`
- **Line 96**: Replace `OkHttpClient httpClient` with `HttpClient httpClient`
- **Lines 108-129**: Rewrite HTTP client initialization using `HttpClient.newBuilder()`
- **Lines 338+**: Replace OkHttp request/response handling with JDK HTTP client equivalents
- Update connection pooling and timeout configuration
- Migrate proxy configuration to JDK HTTP client API

#### 2.3 HTTP Request Handler Updates
- **TDHttpRequestHandler.java**: Replace OkHttp Request/Response with JDK equivalents
- **TDHttpRequestHandlers.java**: Update response body handling
- **TDRequestErrorHandler.java**: Update exception handling for JDK HTTP client exceptions

#### 2.4 Proxy Authentication (ProxyAuthenticator.java)
- **Lines 24-28**: Replace OkHttp proxy classes with JDK HTTP client proxy authentication
- Migrate from `okhttp3.Authenticator` to `java.net.Authenticator`

### Phase 3: Code Migration - Remove Deprecated Guava Functions
**Priority: High - Clean up deprecated methods for major version**

#### 3.1 TDClientApi.java
- **Lines 263-266**: Remove deprecated `jobResult()` method with Guava Function parameter
- **Lines 326-329**: Remove deprecated `getBulkImportErrorRecords()` method with Guava Function parameter
- Clean up any unused Guava imports

#### 3.2 TDHttpRequestHandlers.java
- **Lines 28-35**: Remove deprecated `newByteStreamHandler()` method with Guava Function parameter
- Clean up any unused Guava imports

#### 3.3 TDHttpClient.java
- **Lines 536-539**: Remove deprecated `call()` method with Guava Function parameter
- Clean up any unused Guava imports

### Phase 4: Testing Migration - Replace MockWebServer with JDK Test Infrastructure
**Priority: High - Test compatibility**

#### 4.1 Test Infrastructure Updates
- **TestServerFailures.java**: Replace OkHttp MockWebServer with JDK equivalents
- **TestTDHttpClient.java**: Update all OkHttp-specific test code
- **TestTDClient.java**: Migrate MockWebServer usage
- **TDRequestErrorHandlerTest.java**: Update OkHttp Response/Request test objects

#### 4.2 Test Strategy
- Use Java's built-in `com.sun.net.httpserver.HttpServer` for lightweight testing
- Maintain existing test behavior and coverage
- Ensure test stability with new HTTP client

### Phase 5: Documentation Updates
**Priority: Medium - User communication**

#### 5.1 README.md
- **Line 12**: Update "Java 1.8 or higher" to "Java 17 or higher"
- Add migration note: "For Java 8, use td-client-java-1.x series"
- Update architecture description to mention JDK HTTP client instead of OkHttp

#### 5.2 CHANGES.txt
- Add new release entry for 2.0.0 documenting:
  - Breaking change: Minimum Java version now 17
  - Major change: Replaced OkHttp with JDK HTTP client
  - Removed deprecated Guava Function methods
  - Migration instructions

#### 5.3 CLAUDE.md Updates
- Update architecture documentation to reflect JDK HTTP client
- Remove OkHttp references from dependency shading documentation

### Phase 6: Testing & Validation
**Priority: Critical - Ensure compatibility**

#### 6.1 Comprehensive Testing
- Run full test suite on JDK 17: `mvn clean test`
- Test shaded jar creation: `mvn package`
- Integration testing with staging endpoint
- SSL/TLS compatibility testing (JDK 17 has stricter defaults)
- HTTP client performance regression testing
- Proxy functionality validation with JDK HTTP client

#### 6.2 HTTP Client Feature Parity Verification
- Connection pooling behavior
- Timeout configuration
- Proxy support and authentication
- SSL/TLS configuration
- Error handling compatibility

## Migration Benefits

### OkHttp to JDK HTTP Client Migration
- **Reduced Dependencies**: Eliminates OkHttp and Okio dependencies (~2MB size reduction)
- **Native Integration**: Better integration with JVM and security updates
- **Modern APIs**: Access to Java 11+ HTTP client features (HTTP/2, reactive streams)
- **Simplified Maintenance**: One less external dependency to manage

### JDK 17 Upgrade Benefits
- **Long-term Support**: JDK 17 is an LTS release (supported until 2029)
- **Security**: Latest security patches and improvements
- **Performance**: Better garbage collection and runtime optimizations
- **Modern Features**: Access to language improvements from Java 9-17

## Risk Mitigation

### Critical Risks
1. **HTTP Client Replacement**: Comprehensive feature parity testing and performance benchmarking
2. **SSL/TLS Changes**: JDK 17 has stricter defaults - extensive integration testing required
3. **Test Infrastructure**: MockWebServer replacement could destabilize test suite

### Compatibility Approach
- Maintain identical public API surface (no breaking changes to client usage)
- Preserve all existing configuration options (timeouts, proxy, headers)
- Keep same error handling behavior where possible
- Document any behavioral changes

## Implementation Order
1. **Phase 1**: Build configuration (foundation - enables JDK 17 development)
2. **Phase 2**: HTTP client migration (major architectural change)
3. **Phase 3**: Guava Function cleanup (minor API cleanup)
4. **Phase 4**: Test infrastructure migration (depends on HTTP client changes)
5. **Phase 5**: Documentation updates (can be done in parallel with testing)
6. **Phase 6**: Comprehensive testing and validation (final verification)

## Files to Modify

### Core Infrastructure
- `pom.xml` - Remove OkHttp dependencies, update JDK target, version bump
- `.circleci/config.yml` - CI/CD updates for JDK 17 only

### HTTP Client Migration
- `src/main/java/com/treasuredata/client/TDHttpClient.java` - Replace OkHttp with JDK HTTP client
- `src/main/java/com/treasuredata/client/TDHttpRequestHandler.java` - Update request/response interfaces
- `src/main/java/com/treasuredata/client/TDHttpRequestHandlers.java` - Update response handling
- `src/main/java/com/treasuredata/client/TDRequestErrorHandler.java` - Update exception handling
- `src/main/java/com/treasuredata/client/impl/ProxyAuthenticator.java` - Migrate proxy authentication

### API Cleanup
- `src/main/java/com/treasuredata/client/TDClientApi.java` - Remove deprecated Guava Function methods

### Test Migration
- `src/test/java/com/treasuredata/client/TestTDHttpClient.java` - Update HTTP client tests
- `src/test/java/com/treasuredata/client/TestServerFailures.java` - Replace MockWebServer
- `src/test/java/com/treasuredata/client/TestTDClient.java` - Update integration tests
- `src/test/java/com/treasuredata/client/TDRequestErrorHandlerTest.java` - Update error handling tests

### Documentation
- `README.md` - Update Java version and architecture documentation
- `CHANGES.txt` - Document all breaking changes
- `CLAUDE.md` - Update technical documentation

This comprehensive migration transforms td-client-java into a modern, dependency-light library that fully leverages JDK 17 capabilities while removing the OkHttp dependency for a cleaner, more maintainable codebase.