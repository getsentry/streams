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
}
