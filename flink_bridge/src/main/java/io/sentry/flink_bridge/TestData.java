package io.sentry.flink_bridge;

import java.util.Arrays;
import java.util.List;

/**
 * Test data containing sample JSON documents for testing the Flink gRPC
 * application.
 * This class provides a collection of 50 realistic JSON documents representing
 * various
 * types of events that would typically be processed in a streaming application.
 */
public class TestData {

    /**
     * Returns a list of 50 sample JSON documents as strings.
     * Each document represents a different type of event with realistic data.
     *
     * @return List of JSON strings
     */
    public static List<String> getSampleData() {
        return Arrays.asList(
                "{\"id\": 1, \"event\": \"user_login\", \"timestamp\": 1640995200000, \"user_id\": \"user123\", \"ip\": \"192.168.1.100\"}",
                "{\"id\": 2, \"event\": \"page_view\", \"timestamp\": 1640995260000, \"user_id\": \"user456\", \"page\": \"/home\", \"session_id\": \"sess789\"}",
                "{\"id\": 3, \"event\": \"button_click\", \"timestamp\": 1640995320000, \"user_id\": \"user123\", \"button\": \"submit\", \"form_id\": \"login_form\"}",
                "{\"id\": 4, \"event\": \"api_call\", \"timestamp\": 1640995380000, \"endpoint\": \"/api/users\", \"method\": \"GET\", \"status_code\": 200}",
                "{\"id\": 5, \"event\": \"error\", \"timestamp\": 1640995440000, \"error_type\": \"validation_error\", \"message\": \"Invalid email format\", \"severity\": \"warning\"}",
                "{\"id\": 6, \"event\": \"purchase\", \"timestamp\": 1640995500000, \"user_id\": \"user789\", \"amount\": 29.99, \"currency\": \"USD\", \"product_id\": \"prod123\"}",
                "{\"id\": 7, \"event\": \"search\", \"timestamp\": 1640995560000, \"user_id\": \"user456\", \"query\": \"flink streaming\", \"results_count\": 15}",
                "{\"id\": 8, \"event\": \"download\", \"timestamp\": 1640995620000, \"user_id\": \"user123\", \"file_name\": \"document.pdf\", \"file_size\": 2048576}",
                "{\"id\": 9, \"event\": \"notification\", \"timestamp\": 1640995680000, \"user_id\": \"user789\", \"type\": \"email\", \"subject\": \"Welcome to our platform\"}",
                "{\"id\": 10, \"event\": \"logout\", \"timestamp\": 1640995740000, \"user_id\": \"user123\", \"session_duration\": 1800}",
                "{\"id\": 11, \"event\": \"profile_update\", \"timestamp\": 1640995800000, \"user_id\": \"user456\", \"field\": \"avatar\", \"old_value\": \"default.jpg\"}",
                "{\"id\": 12, \"event\": \"comment\", \"timestamp\": 1640995860000, \"user_id\": \"user789\", \"post_id\": \"post123\", \"content\": \"Great article!\"}",
                "{\"id\": 13, \"event\": \"like\", \"timestamp\": 1640995920000, \"user_id\": \"user123\", \"post_id\": \"post456\", \"reaction\": \"thumbs_up\"}",
                "{\"id\": 14, \"event\": \"share\", \"timestamp\": 1640995980000, \"user_id\": \"user456\", \"post_id\": \"post789\", \"platform\": \"twitter\"}",
                "{\"id\": 15, \"event\": \"follow\", \"timestamp\": 1640996040000, \"follower_id\": \"user123\", \"following_id\": \"user789\"}",
                "{\"id\": 16, \"event\": \"message\", \"timestamp\": 1640996100000, \"sender_id\": \"user456\", \"receiver_id\": \"user123\", \"message_type\": \"text\"}",
                "{\"id\": 17, \"event\": \"upload\", \"timestamp\": 1640996160000, \"user_id\": \"user789\", \"file_type\": \"image\", \"file_size\": 1048576, \"mime_type\": \"image/jpeg\"}",
                "{\"id\": 18, \"event\": \"rating\", \"timestamp\": 1640996220000, \"user_id\": \"user123\", \"item_id\": \"item456\", \"rating\": 5, \"review\": \"Excellent product\"}",
                "{\"id\": 19, \"event\": \"subscription\", \"timestamp\": 1640996280000, \"user_id\": \"user456\", \"plan\": \"premium\", \"billing_cycle\": \"monthly\", \"price\": 9.99}",
                "{\"id\": 20, \"event\": \"unsubscribe\", \"timestamp\": 1640996340000, \"user_id\": \"user789\", \"newsletter\": \"weekly_digest\", \"reason\": \"too_many_emails\"}",
                "{\"id\": 21, \"event\": \"password_reset\", \"timestamp\": 1640996400000, \"user_id\": \"user123\", \"reset_method\": \"email\", \"ip_address\": \"192.168.1.101\"}",
                "{\"id\": 22, \"event\": \"two_factor\", \"timestamp\": 1640996460000, \"user_id\": \"user456\", \"method\": \"sms\", \"success\": true, \"device\": \"mobile\"}",
                "{\"id\": 23, \"event\": \"session_expiry\", \"timestamp\": 1640996520000, \"user_id\": \"user789\", \"session_id\": \"sess456\", \"duration\": 3600}",
                "{\"id\": 24, \"event\": \"feature_flag\", \"timestamp\": 1640996580000, \"user_id\": \"user123\", \"flag_name\": \"dark_mode\", \"enabled\": true}",
                "{\"id\": 25, \"event\": \"ab_test\", \"timestamp\": 1640996640000, \"user_id\": \"user456\", \"test_name\": \"button_color\", \"variant\": \"blue\", \"conversion\": false}",
                "{\"id\": 26, \"event\": \"performance\", \"timestamp\": 1640996700000, \"page_load_time\": 1250, \"user_id\": \"user789\", \"browser\": \"chrome\", \"device\": \"desktop\"}",
                "{\"id\": 27, \"event\": \"crash\", \"timestamp\": 1640996760000, \"error_type\": \"null_pointer\", \"stack_trace\": \"java.lang.NullPointerException\", \"user_id\": \"user123\"}",
                "{\"id\": 28, \"event\": \"database_query\", \"timestamp\": 1640996820000, \"query_type\": \"SELECT\", \"table\": \"users\", \"execution_time\": 45, \"rows_returned\": 100}",
                "{\"id\": 29, \"event\": \"cache_hit\", \"timestamp\": 1640996880000, \"cache_key\": \"user_profile_456\", \"cache_type\": \"redis\", \"response_time\": 2}",
                "{\"id\": 30, \"event\": \"cache_miss\", \"timestamp\": 1640996940000, \"cache_key\": \"user_preferences_789\", \"cache_type\": \"memcached\", \"fallback\": \"database\"}",
                "{\"id\": 31, \"event\": \"queue_message\", \"timestamp\": 1640997000000, \"queue_name\": \"email_queue\", \"message_size\": 1024, \"priority\": \"high\"}",
                "{\"id\": 32, \"event\": \"batch_processing\", \"timestamp\": 1640997060000, \"batch_id\": \"batch_123\", \"records_processed\": 1000, \"processing_time\": 5000}",
                "{\"id\": 33, \"event\": \"stream_processing\", \"timestamp\": 1640997120000, \"stream_id\": \"stream_456\", \"records_consumed\": 500, \"latency\": 100}",
                "{\"id\": 34, \"event\": \"microservice_call\", \"timestamp\": 1640997180000, \"service_name\": \"user_service\", \"endpoint\": \"/users/123\", \"response_time\": 150}",
                "{\"id\": 35, \"event\": \"circuit_breaker\", \"timestamp\": 1640997240000, \"service_name\": \"payment_service\", \"state\": \"open\", \"failure_count\": 10}",
                "{\"id\": 36, \"event\": \"rate_limit\", \"timestamp\": 1640997300000, \"user_id\": \"user456\", \"endpoint\": \"/api/orders\", \"limit\": 100, \"current\": 101}",
                "{\"id\": 37, \"event\": \"security_alert\", \"timestamp\": 1640997360000, \"alert_type\": \"suspicious_login\", \"user_id\": \"user789\", \"ip_address\": \"203.0.113.1\", \"risk_score\": 85}",
                "{\"id\": 38, \"event\": \"compliance_check\", \"timestamp\": 1640997420000, \"check_type\": \"gdpr\", \"user_id\": \"user123\", \"data_category\": \"personal_info\", \"status\": \"compliant\"}",
                "{\"id\": 39, \"event\": \"backup\", \"timestamp\": 1640997480000, \"backup_type\": \"full\", \"database\": \"user_db\", \"size_gb\": 50, \"duration_minutes\": 30}",
                "{\"id\": 40, \"event\": \"deployment\", \"timestamp\": 1640997540000, \"service_name\": \"api_gateway\", \"version\": \"v2.1.0\", \"environment\": \"production\", \"status\": \"successful\"}",
                "{\"id\": 41, \"event\": \"health_check\", \"timestamp\": 1640997600000, \"service_name\": \"user_service\", \"status\": \"healthy\", \"response_time\": 25, \"memory_usage\": 75}",
                "{\"id\": 42, \"event\": \"scaling\", \"timestamp\": 1640997660000, \"service_name\": \"web_server\", \"action\": \"scale_up\", \"instances\": 5, \"trigger\": \"cpu_usage\"}",
                "{\"id\": 43, \"event\": \"load_balancer\", \"timestamp\": 1640997720000, \"lb_name\": \"app_lb\", \"backend_health\": \"healthy\", \"active_connections\": 150, \"requests_per_second\": 1000}",
                "{\"id\": 44, \"event\": \"ssl_certificate\", \"timestamp\": 1640997780000, \"domain\": \"example.com\", \"expiry_date\": \"2024-12-31\", \"days_until_expiry\": 300, \"status\": \"valid\"}",
                "{\"id\": 45, \"event\": \"monitoring_alert\", \"timestamp\": 1640997840000, \"alert_name\": \"high_cpu_usage\", \"severity\": \"critical\", \"value\": 95, \"threshold\": 80}",
                "{\"id\": 46, \"event\": \"log_rotation\", \"timestamp\": 1640997900000, \"log_file\": \"application.log\", \"old_size_mb\": 100, \"new_size_mb\": 0, \"compression\": \"gzip\"}",
                "{\"id\": 47, \"event\": \"data_migration\", \"timestamp\": 1640997960000, \"source\": \"old_database\", \"destination\": \"new_database\", \"records_migrated\": 1000000, \"status\": \"in_progress\"}",
                "{\"id\": 48, \"event\": \"api_versioning\", \"timestamp\": 1640998020000, \"endpoint\": \"/api/v1/users\", \"new_version\": \"v2\", \"deprecation_date\": \"2024-06-01\", \"migration_guide\": \"https://docs.example.com/migrate\"}",
                "{\"id\": 49, \"event\": \"feature_deprecation\", \"timestamp\": 1640998080000, \"feature_name\": \"legacy_auth\", \"deprecation_date\": \"2024-03-01\", \"replacement\": \"oauth2\", \"migration_deadline\": \"2024-09-01\"}",
                "{\"id\": 50, \"event\": \"system_maintenance\", \"timestamp\": 1640998140000, \"maintenance_type\": \"scheduled\", \"duration_minutes\": 120, \"affected_services\": [\"user_service\", \"payment_service\"], \"status\": \"completed\"}");
    }

    /**
     * Returns a list of 50 sample JSON metric documents as strings.
     * Each document represents a different metric compliant with Sentry's
     * ingest-metrics.v1 schema.
     * Schema fields: org_id, project_id, name, tags, timestamp, type,
     * retention_days, value
     *
     * @return List of JSON metric strings
     */
    public static List<String> getMetrics() {
        return Arrays.asList(
                "{\"org_id\": 1, \"project_id\": 101, \"name\": \"s:sessions/user@none\", \"tags\": {\"sdk\": \"raven-node/2.6.3\", \"environment\": \"production\", \"release\": \"v1.0.0\"}, \"timestamp\": 1640995200000, \"type\": \"s\", \"retention_days\": 90, \"value\": [1617781333]}",
                "{\"org_id\": 1, \"project_id\": 102, \"name\": \"c:custom/error_rate@none\", \"tags\": {\"sdk\": \"sentry-java/5.0.0\", \"environment\": \"staging\", \"release\": \"v1.1.0\"}, \"timestamp\": 1640995260000, \"type\": \"c\", \"retention_days\": 90, \"value\": [42]}",
                "{\"org_id\": 2, \"project_id\": 201, \"name\": \"d:custom/page_load@millisecond\", \"tags\": {\"sdk\": \"sentry-python/1.0.0\", \"environment\": \"production\", \"release\": \"v2.0.0\"}, \"timestamp\": 1640995320000, \"type\": \"d\", \"retention_days\": 90, \"value\": [1250, 980, 1100]}",
                "{\"org_id\": 2, \"project_id\": 202, \"name\": \"g:custom/memory_usage@byte\", \"tags\": {\"sdk\": \"sentry-js/7.0.0\", \"environment\": \"development\", \"release\": \"v1.5.0\"}, \"timestamp\": 1640995380000, \"type\": \"g\", \"retention_days\": 90, \"value\": [1073741824]}",
                "{\"org_id\": 3, \"project_id\": 301, \"name\": \"s:sessions/user@none\", \"tags\": {\"sdk\": \"sentry-ios/8.0.0\", \"environment\": \"production\", \"release\": \"v3.0.0\"}, \"timestamp\": 1640995440000, \"type\": \"s\", \"retention_days\": 90, \"value\": [987654321]}",
                "{\"org_id\": 3, \"project_id\": 302, \"name\": \"c:custom/api_calls@none\", \"tags\": {\"sdk\": \"sentry-android/9.0.0\", \"environment\": \"staging\", \"release\": \"v2.1.0\"}, \"timestamp\": 1640995500000, \"type\": \"c\", \"retention_days\": 90, \"value\": [156]}",
                "{\"org_id\": 4, \"project_id\": 401, \"name\": \"d:custom/database_query@millisecond\", \"tags\": {\"sdk\": \"sentry-php/4.0.0\", \"environment\": \"production\", \"release\": \"v1.8.0\"}, \"timestamp\": 1640995560000, \"type\": \"d\", \"retention_days\": 90, \"value\": [45, 67, 89, 34]}",
                "{\"org_id\": 4, \"project_id\": 402, \"name\": \"g:custom/cpu_usage@percent\", \"tags\": {\"sdk\": \"sentry-ruby/6.0.0\", \"environment\": \"development\", \"release\": \"v2.2.0\"}, \"timestamp\": 1640995620000, \"type\": \"g\", \"retention_days\": 90, \"value\": [75]}",
                "{\"org_id\": 5, \"project_id\": 501, \"name\": \"s:sessions/user@none\", \"tags\": {\"sdk\": \"sentry-go/10.0.0\", \"environment\": \"production\", \"release\": \"v4.0.0\"}, \"timestamp\": 1640995680000, \"type\": \"s\", \"retention_days\": 90, \"value\": [234567890]}",
                "{\"org_id\": 5, \"project_id\": 502, \"name\": \"c:custom/error_count@none\", \"tags\": {\"sdk\": \"sentry-dotnet/11.0.0\", \"environment\": \"staging\", \"release\": \"v3.1.0\"}, \"timestamp\": 1640995740000, \"type\": \"c\", \"retention_days\": 90, \"value\": [23]}",
                "{\"org_id\": 6, \"project_id\": 601, \"name\": \"d:custom/response_time@millisecond\", \"tags\": {\"sdk\": \"sentry-elixir/12.0.0\", \"environment\": \"production\", \"release\": \"v5.0.0\"}, \"timestamp\": 1640995800000, \"type\": \"d\", \"retention_days\": 90, \"value\": [200, 180, 220, 195]}",
                "{\"org_id\": 6, \"project_id\": 602, \"name\": \"g:custom/disk_usage@percent\", \"tags\": {\"sdk\": \"sentry-clojure/13.0.0\", \"environment\": \"staging\", \"release\": \"v4.1.0\"}, \"timestamp\": 1640995860000, \"type\": \"g\", \"retention_days\": 90, \"value\": [65]}",
                "{\"org_id\": 7, \"project_id\": 701, \"name\": \"s:sessions/user@none\", \"tags\": {\"sdk\": \"sentry-scala/14.0.0\", \"environment\": \"production\", \"release\": \"v6.0.0\"}, \"timestamp\": 1640995920000, \"type\": \"s\", \"retention_days\": 90, \"value\": [345678901]}",
                "{\"org_id\": 7, \"project_id\": 702, \"name\": \"c:custom/request_count@none\", \"tags\": {\"sdk\": \"sentry-kotlin/15.0.0\", \"environment\": \"development\", \"release\": \"v5.1.0\"}, \"timestamp\": 1640995980000, \"type\": \"c\", \"retention_days\": 90, \"value\": [789]}",
                "{\"org_id\": 8, \"project_id\": 801, \"name\": \"d:custom/processing_time@millisecond\", \"tags\": {\"sdk\": \"sentry-swift/16.0.0\", \"environment\": \"production\", \"release\": \"v7.0.0\"}, \"timestamp\": 1640996040000, \"type\": \"d\", \"retention_days\": 90, \"value\": [150, 175, 160]}",
                "{\"org_id\": 8, \"project_id\": 802, \"name\": \"g:custom/network_io@bytes_per_second\", \"tags\": {\"sdk\": \"sentry-rust/17.0.0\", \"environment\": \"staging\", \"release\": \"v6.1.0\"}, \"timestamp\": 1640996100000, \"type\": \"g\", \"retention_days\": 90, \"value\": [1048576]}",
                "{\"org_id\": 9, \"project_id\": 901, \"name\": \"s:sessions/user@none\", \"tags\": {\"sdk\": \"sentry-cpp/18.0.0\", \"environment\": \"production\", \"release\": \"v8.0.0\"}, \"timestamp\": 1640996160000, \"type\": \"s\", \"retention_days\": 90, \"value\": [456789012]}",
                "{\"org_id\": 9, \"project_id\": 902, \"name\": \"c:custom/exception_count@none\", \"tags\": {\"sdk\": \"sentry-csharp/19.0.0\", \"environment\": \"development\", \"release\": \"v7.1.0\"}, \"timestamp\": 1640996220000, \"type\": \"c\", \"retention_days\": 90, \"value\": [12]}",
                "{\"org_id\": 10, \"project_id\": 1001, \"name\": \"d:custom/queue_depth@none\", \"tags\": {\"sdk\": \"sentry-fsharp/20.0.0\", \"environment\": \"production\", \"release\": \"v9.0.0\"}, \"timestamp\": 1640996280000, \"type\": \"d\", \"retention_days\": 90, \"value\": [25, 30, 28, 35]}",
                "{\"org_id\": 10, \"project_id\": 1002, \"name\": \"g:custom/thread_count@none\", \"tags\": {\"sdk\": \"sentry-vb/21.0.0\", \"environment\": \"staging\", \"release\": \"v8.1.0\"}, \"timestamp\": 1640996340000, \"type\": \"g\", \"retention_days\": 90, \"value\": [16]}",
                "{\"org_id\": 11, \"project_id\": 1101, \"name\": \"s:sessions/user@none\", \"tags\": {\"sdk\": \"sentry-perl/22.0.0\", \"environment\": \"production\", \"release\": \"v10.0.0\"}, \"timestamp\": 1640996400000, \"type\": \"s\", \"retention_days\": 90, \"value\": [567890123]}",
                "{\"org_id\": 11, \"project_id\": 1102, \"name\": \"c:custom/transaction_count@none\", \"tags\": {\"sdk\": \"sentry-python/23.0.0\", \"environment\": \"staging\", \"release\": \"v9.1.0\"}, \"timestamp\": 1640996460000, \"type\": \"c\", \"retention_days\": 90, \"value\": [456]}",
                "{\"org_id\": 12, \"project_id\": 1201, \"name\": \"d:custom/cache_hit_rate@percent\", \"tags\": {\"sdk\": \"sentry-java/24.0.0\", \"environment\": \"production\", \"release\": \"v11.0.0\"}, \"timestamp\": 1640996520000, \"type\": \"d\", \"retention_days\": 90, \"value\": [85, 92, 78, 89]}",
                "{\"org_id\": 12, \"project_id\": 1202, \"name\": \"g:custom/active_connections@none\", \"tags\": {\"sdk\": \"sentry-js/25.0.0\", \"environment\": \"development\", \"release\": \"v10.1.0\"}, \"timestamp\": 1640996580000, \"type\": \"g\", \"retention_days\": 90, \"value\": [128]}",
                "{\"org_id\": 13, \"project_id\": 1301, \"name\": \"s:sessions/user@none\", \"tags\": {\"sdk\": \"sentry-ios/26.0.0\", \"environment\": \"production\", \"release\": \"v12.0.0\"}, \"timestamp\": 1640996640000, \"type\": \"s\", \"retention_days\": 90, \"value\": [678901234]}",
                "{\"org_id\": 13, \"project_id\": 1302, \"name\": \"c:custom/feature_flag_usage@none\", \"tags\": {\"sdk\": \"sentry-android/27.0.0\", \"environment\": \"staging\", \"release\": \"v11.1.0\"}, \"timestamp\": 1640996700000, \"type\": \"c\", \"retention_days\": 90, \"value\": [89]}",
                "{\"org_id\": 14, \"project_id\": 1401, \"name\": \"d:custom/authentication_time@millisecond\", \"tags\": {\"sdk\": \"sentry-php/28.0.0\", \"environment\": \"production\", \"release\": \"v13.0.0\"}, \"timestamp\": 1640996760000, \"type\": \"d\", \"retention_days\": 90, \"value\": [320, 280, 350, 290]}",
                "{\"org_id\": 14, \"project_id\": 1402, \"name\": \"g:custom/queue_processing_rate@messages_per_second\", \"tags\": {\"sdk\": \"sentry-ruby/29.0.0\", \"environment\": \"development\", \"release\": \"v12.1.0\"}, \"timestamp\": 1640996820000, \"type\": \"g\", \"retention_days\": 90, \"value\": [150]}",
                "{\"org_id\": 15, \"project_id\": 1501, \"name\": \"s:sessions/user@none\", \"tags\": {\"sdk\": \"sentry-go/30.0.0\", \"environment\": \"production\", \"release\": \"v14.0.0\"}, \"timestamp\": 1640996880000, \"type\": \"s\", \"retention_days\": 90, \"value\": [789012345]}",
                "{\"org_id\": 15, \"project_id\": 1502, \"name\": \"c:custom/ab_test_conversion@none\", \"tags\": {\"sdk\": \"sentry-dotnet/31.0.0\", \"environment\": \"staging\", \"release\": \"v13.1.0\"}, \"timestamp\": 1640996940000, \"type\": \"c\", \"retention_days\": 90, \"value\": [67]}",
                "{\"org_id\": 16, \"project_id\": 1601, \"name\": \"d:custom/rendering_time@millisecond\", \"tags\": {\"sdk\": \"sentry-elixir/32.0.0\", \"environment\": \"production\", \"release\": \"v15.0.0\"}, \"timestamp\": 1640997000000, \"type\": \"d\", \"retention_days\": 90, \"value\": [45, 52, 48, 55]}",
                "{\"org_id\": 16, \"project_id\": 1602, \"name\": \"g:custom/event_queue_size@none\", \"tags\": {\"sdk\": \"sentry-clojure/33.0.0\", \"environment\": \"staging\", \"release\": \"v14.1.0\"}, \"timestamp\": 1640997060000, \"type\": \"g\", \"retention_days\": 90, \"value\": [256]}",
                "{\"org_id\": 17, \"project_id\": 1701, \"name\": \"s:sessions/user@none\", \"tags\": {\"sdk\": \"sentry-scala/34.0.0\", \"environment\": \"production\", \"release\": \"v16.0.0\"}, \"timestamp\": 1640997120000, \"type\": \"s\", \"retention_days\": 90, \"value\": [890123456]}",
                "{\"org_id\": 17, \"project_id\": 1702, \"name\": \"c:custom/performance_score@none\", \"tags\": {\"sdk\": \"sentry-kotlin/35.0.0\", \"environment\": \"development\", \"release\": \"v15.1.0\"}, \"timestamp\": 1640997180000, \"type\": \"c\", \"retention_days\": 90, \"value\": [92]}",
                "{\"org_id\": 18, \"project_id\": 1801, \"name\": \"d:custom/compilation_time@millisecond\", \"tags\": {\"sdk\": \"sentry-swift/36.0.0\", \"environment\": \"production\", \"release\": \"v17.0.0\"}, \"timestamp\": 1640997240000, \"type\": \"d\", \"retention_days\": 90, \"value\": [1200, 1350, 1100]}",
                "{\"org_id\": 18, \"project_id\": 1802, \"name\": \"g:custom/compiler_memory@megabyte\", \"tags\": {\"sdk\": \"sentry-rust/37.0.0\", \"environment\": \"staging\", \"release\": \"v16.1.0\"}, \"timestamp\": 1640997300000, \"type\": \"g\", \"retention_days\": 90, \"value\": [512]}",
                "{\"org_id\": 19, \"project_id\": 1901, \"name\": \"s:sessions/user@none\", \"tags\": {\"sdk\": \"sentry-cpp/38.0.0\", \"environment\": \"production\", \"release\": \"v18.0.0\"}, \"timestamp\": 1640997360000, \"type\": \"s\", \"retention_days\": 90, \"value\": [901234567]}",
                "{\"org_id\": 19, \"project_id\": 1902, \"name\": \"c:custom/build_success_rate@percent\", \"tags\": {\"sdk\": \"sentry-csharp/39.0.0\", \"environment\": \"development\", \"release\": \"v17.1.0\"}, \"timestamp\": 1640997420000, \"type\": \"c\", \"retention_days\": 90, \"value\": [98]}",
                "{\"org_id\": 20, \"project_id\": 2001, \"name\": \"d:custom/deployment_time@second\", \"tags\": {\"sdk\": \"sentry-fsharp/40.0.0\", \"environment\": \"production\", \"release\": \"v19.0.0\"}, \"timestamp\": 1640997480000, \"type\": \"d\", \"retention_days\": 90, \"value\": [45, 52, 48]}",
                "{\"org_id\": 20, \"project_id\": 2002, \"name\": \"g:custom/container_cpu@percent\", \"tags\": {\"sdk\": \"sentry-vb/41.0.0\", \"environment\": \"staging\", \"release\": \"v18.1.0\"}, \"timestamp\": 1640997540000, \"type\": \"g\", \"retention_days\": 90, \"value\": [35]}",
                "{\"org_id\": 21, \"project_id\": 2101, \"name\": \"s:sessions/user@none\", \"tags\": {\"sdk\": \"sentry-perl/42.0.0\", \"environment\": \"production\", \"release\": \"v20.0.0\"}, \"timestamp\": 1640997600000, \"type\": \"s\", \"retention_days\": 90, \"value\": [123456789]}",
                "{\"org_id\": 21, \"project_id\": 2102, \"name\": \"c:custom/security_scan_count@none\", \"tags\": {\"sdk\": \"sentry-python/43.0.0\", \"environment\": \"staging\", \"release\": \"v19.1.0\"}, \"timestamp\": 1640997660000, \"type\": \"c\", \"retention_days\": 90, \"value\": [15]}",
                "{\"org_id\": 22, \"project_id\": 2201, \"name\": \"d:custom/encryption_time@millisecond\", \"tags\": {\"sdk\": \"sentry-java/44.0.0\", \"environment\": \"production\", \"release\": \"v21.0.0\"}, \"timestamp\": 1640997720000, \"type\": \"d\", \"retention_days\": 90, \"value\": [25, 30, 28, 35]}",
                "{\"org_id\": 22, \"project_id\": 2202, \"name\": \"g:custom/ssl_cert_expiry@days\", \"tags\": {\"sdk\": \"sentry-js/45.0.0\", \"environment\": \"development\", \"release\": \"v20.1.0\"}, \"timestamp\": 1640997780000, \"type\": \"g\", \"retention_days\": 90, \"value\": [45]}",
                "{\"org_id\": 23, \"project_id\": 2301, \"name\": \"s:sessions/user@none\", \"tags\": {\"sdk\": \"sentry-ios/46.0.0\", \"environment\": \"production\", \"release\": \"v22.0.0\"}, \"timestamp\": 1640997840000, \"type\": \"s\", \"retention_days\": 90, \"value\": [234567890]}",
                "{\"org_id\": 23, \"project_id\": 2302, \"name\": \"c:custom/backup_success_count@none\", \"tags\": {\"sdk\": \"sentry-android/47.0.0\", \"environment\": \"staging\", \"release\": \"v21.1.0\"}, \"timestamp\": 1640997900000, \"type\": \"c\", \"retention_days\": 90, \"value\": [28]}",
                "{\"org_id\": 24, \"project_id\": 2401, \"name\": \"d:custom/restore_time@minute\", \"tags\": {\"sdk\": \"sentry-php/48.0.0\", \"environment\": \"production\", \"release\": \"v23.0.0\"}, \"timestamp\": 1640997960000, \"type\": \"d\", \"retention_days\": 90, \"value\": [15, 18, 12]}",
                "{\"org_id\": 24, \"project_id\": 2402, \"name\": \"g:custom/backup_size@gigabyte\", \"tags\": {\"sdk\": \"sentry-ruby/49.0.0\", \"environment\": \"development\", \"release\": \"v22.1.0\"}, \"timestamp\": 1640998020000, \"type\": \"g\", \"retention_days\": 90, \"value\": [25]}",
                "{\"org_id\": 25, \"project_id\": 2501, \"name\": \"s:sessions/user@none\", \"tags\": {\"sdk\": \"sentry-go/50.0.0\", \"environment\": \"production\", \"release\": \"v24.0.0\"}, \"timestamp\": 1640998080000, \"type\": \"s\", \"retention_days\": 90, \"value\": [345678901]}",
                "{\"org_id\": 25, \"project_id\": 2502, \"name\": \"c:custom/migration_success@none\", \"tags\": {\"sdk\": \"sentry-dotnet/51.0.0\", \"environment\": \"staging\", \"release\": \"v23.1.0\"}, \"timestamp\": 1640998140000, \"type\": \"c\", \"retention_days\": 90, \"value\": [34]}");
    }

}
