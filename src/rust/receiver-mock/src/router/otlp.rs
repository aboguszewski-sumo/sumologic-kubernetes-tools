use std::collections::HashMap;
use std::io::Cursor;
use std::iter::FromIterator;

use crate::metadata::Metadata;
use crate::metrics::lines_to_samples;
use crate::metrics::MetricsHandleResult;
use crate::options;
use crate::router::*;
use actix_web::{web, HttpRequest, HttpResponse, Responder};
use opentelemetry_proto::tonic::logs::v1 as logsv1;
use opentelemetry_proto::tonic::metrics::v1 as metricsv1;
use prost::Message;

const OTLP_PROTOBUF_FORMAT_CONTENT_TYPE: &str = "application/x-protobuf";

// TODO: consider moving the whole module to a separate directory

pub async fn handler_receiver_otlp_logs(
    req: HttpRequest,
    body: web::Bytes,
    app_state: web::Data<AppState>,
    opts: web::Data<options::Options>,
) -> impl Responder {
    let remote_address = get_address(&req);

    let content_type = get_content_type(&req);

    if let Some(response) = try_dropping_data(&opts, &content_type) {
        return response;
    }

    match content_type.as_str() {
        OTLP_PROTOBUF_FORMAT_CONTENT_TYPE => {
            let log_data: logsv1::LogsData = match logsv1::LogsData::decode(&mut Cursor::new(body)) {
                Ok(data) => data,
                Err(_) => return HttpResponse::BadRequest().body("Unable to parse body"),
            };
            for resource_logs in log_data.resource_logs {
                let metadata = get_otlp_metadata_from_logs(&resource_logs);
                let lines = get_otlp_lines_from_logs(&resource_logs);

                app_state.add_log_lines(
                    lines.iter().map(|x| x.as_str()),
                    metadata,
                    remote_address,
                    &opts,
                );

                if opts.print.logs {
                    for line in lines {
                        println!("log => {}", line);
                    }
                }
            }
        }
        &_ => {
            return get_invalid_header_response(&content_type);
        }
    }

    HttpResponse::Ok().body("")
}

fn get_otlp_metadata_from_logs(resource_logs: &logsv1::ResourceLogs) -> Metadata {
    match &resource_logs.resource {
        Some(resource) => HashMap::from_iter(resource.attributes.iter().map(|kv| {
            (
                kv.key.clone(),
                match &kv.value {
                    Some(value) => format::anyvalue_to_string(value),
                    None => String::new(),
                },
            )
        })),
        None => Metadata::new(),
    }
}

fn get_otlp_lines_from_logs(resource_logs: &logsv1::ResourceLogs) -> Vec<String> {
    resource_logs
        .instrumentation_library_logs
        .iter()
        .map(|ill| ill.log_records.iter())
        .flatten()
        .map(|log_record| match &log_record.body {
            Some(body) => format::anyvalue_to_string(&body),
            None => String::new(),
        })
        .collect()
}

pub async fn handler_receiver_otlp_metrics(
    req: HttpRequest,
    body: web::Bytes,
    app_state: web::Data<AppState>,
    opts: web::Data<options::Options>,
) -> impl Responder {
    let remote_address = get_address(&req);

    let content_type = get_content_type(&req);

    if let Some(response) = try_dropping_data(&opts, &content_type) {
        return response;
    }

    match content_type.as_str() {
        OTLP_PROTOBUF_FORMAT_CONTENT_TYPE => {
            let metrics_data: metricsv1::MetricsData = match metricsv1::MetricsData::decode(&mut Cursor::new(body)) {
                Ok(data) => data,
                Err(_) => return HttpResponse::BadRequest().body("Unable to parse body"),
            };
            let mut result = MetricsHandleResult::new();

            // TODO: Consider giving it some basic capacity to avoid too many allocations.
            let mut lines = vec![];
            for resource_metrics in metrics_data.resource_metrics {
                let resource_attributes = &resource_metrics.resource.unwrap().attributes;
                for instrumentation_lib_metrics in resource_metrics.instrumentation_library_metrics {
                    for metric in instrumentation_lib_metrics.metrics {
                        // Translating the metrics to prometheus format allows us to use the already existing API for parsing metrics into samples.
                        let metric_str_vec = format::otlp_metric_to_prometheus_string(&metric, resource_attributes);

                        for m in metric_str_vec {
                            if opts.print.metrics {
                                println!("metrics => {}", m);
                            }
                            if opts.store_metrics {
                                lines.push(m);
                            }
                        }

                        result.handle_metric(metric.name);
                        result.handle_ip(remote_address);
                    }
                }
            }

            if opts.store_metrics {
                result.metrics_samples = lines_to_samples(lines);
            }

            app_state.add_metrics_result(result, &opts);
        }
        &_ => {
            return get_invalid_header_response(&content_type);
        }
    }

    HttpResponse::Ok().body("")
}

mod format {
    use std::collections::HashMap;

    use itertools::Itertools;
    use metricsv1::number_data_point;
    use metricsv1::{Gauge, Sum};
    use opentelemetry_proto::tonic::common::v1::{self as commonv1, AnyValue};
    use opentelemetry_proto::tonic::metrics::v1 as metricsv1;

    type Attributes = [commonv1::KeyValue];

    const NANOS_IN_MILLIS: u64 = 1_000_000;

    pub fn otlp_metric_to_prometheus_string(metric: &metricsv1::Metric, attributes: &Attributes) -> Vec<String> {
        if let Some(data) = &metric.data {
            match data {
                // TODO: Support all the types
                metricsv1::metric::Data::Gauge(g) => gauge_to_lines(g, &metric.name, attributes),
                metricsv1::metric::Data::Sum(s) => sum_to_lines(s, &metric.name, attributes),
                _ => todo!(),
            }
        } else {
            vec![String::new()]
        }
    }

    fn gauge_to_lines(gauge: &Gauge, name: &str, attributes: &Attributes) -> Vec<String> {
        gauge
            .data_points
            .iter()
            .map(|dp| number_datapoint_to_line(dp, name, attributes))
            .collect()
    }

    fn sum_to_lines(sum: &Sum, name: &str, attributes: &Attributes) -> Vec<String> {
        sum.data_points
            .iter()
            .map(|dp| number_datapoint_to_line(dp, name, attributes))
            .collect()
    }

    fn number_datapoint_to_line(dp: &metricsv1::NumberDataPoint, name: &str, attributes: &Attributes) -> String {
        if let Some(val) = &dp.value {
            let labels_str = tags_to_string(&dp.attributes, attributes);
            let timestamp = get_number_datapoint_timestamp_millis(dp);
            match val {
                number_data_point::Value::AsDouble(x) => format_line(name, &labels_str, *x, timestamp),
                number_data_point::Value::AsInt(x) => format_line(name, &labels_str, *x, timestamp),
            }
        } else {
            String::new()
        }
    }

    fn get_number_datapoint_timestamp_millis(dp: &metricsv1::NumberDataPoint) -> u64 {
        dp.time_unix_nano / NANOS_IN_MILLIS
    }

    fn format_line<T: std::fmt::Display>(name: &str, labels_str: &str, value: T, timestamp: u64) -> String {
        format!("{}{} {} {}", name, labels_str, value, timestamp)
    }

    fn tags_to_string(attrs: &Attributes, labels: &Attributes) -> String {
        let map: HashMap<&String, &Option<AnyValue>> = attrs
            .iter()
            .chain(labels.iter())
            .map(|kv| (&kv.key, &kv.value))
            .collect();

        let labels_str = map
            .iter()
            .map(|(k, v)| key_value_to_string(k, v.as_ref().unwrap()))
            .sorted()
            .join(",");

        format!("{{{}}}", labels_str)
    }

    fn key_value_to_string(key: &str, value: &AnyValue) -> String {
        format!("{}={}", key, anyvalue_to_string(value))
    }

    pub fn anyvalue_to_string(anyvalue: &commonv1::AnyValue) -> String {
        let value = match &anyvalue.value {
            Some(v) => v,
            None => return String::new(),
        };
        let s = match value {
            commonv1::any_value::Value::StringValue(inner) => inner.clone(),
            commonv1::any_value::Value::BoolValue(inner) => inner.to_string(),
            commonv1::any_value::Value::IntValue(inner) => inner.to_string(),
            commonv1::any_value::Value::DoubleValue(inner) => inner.to_string(),
            _ => String::new(),
        };

        return s;
    }

    #[cfg(test)]
    pub mod test {
        use commonv1::AnyValue;
        use commonv1::KeyValue;
        use metricsv1::NumberDataPoint;
        use opentelemetry_proto::tonic::common::v1 as commonv1;
        use opentelemetry_proto::tonic::metrics::v1 as metricsv1;
        use opentelemetry_proto::tonic::metrics::v1::Gauge;
        use opentelemetry_proto::tonic::metrics::v1::Metric;
        use opentelemetry_proto::tonic::metrics::v1::Sum;

        use super::otlp_metric_to_prometheus_string;
        use super::NANOS_IN_MILLIS;
        use super::{key_value_to_string, number_datapoint_to_line, tags_to_string};

        fn get_string_anyvalue(string: &str) -> AnyValue {
            AnyValue {
                value: Some(commonv1::any_value::Value::StringValue(string.to_string())),
            }
        }

        fn pairs_to_keyvalue(pairs: Vec<(&str, AnyValue)>) -> Vec<KeyValue> {
            pairs
                .into_iter()
                .map(|(k, v)| KeyValue {
                    key: k.to_string(),
                    value: Some(v),
                })
                .collect()
        }

        fn get_sample_resource_attrs() -> Vec<KeyValue> {
            pairs_to_keyvalue(vec![
                ("key1", get_string_anyvalue("value1")),
                ("key2", get_string_anyvalue("value2")),
            ])
        }

        fn get_sample_dp_attrs() -> Vec<KeyValue> {
            pairs_to_keyvalue(vec![
                ("key2", get_string_anyvalue("surprise")),
                ("key3", get_string_anyvalue("value3")),
            ])
        }

        #[test]
        fn otlp_format_key_value_test() {
            let key = "key".to_string();
            let value = get_string_anyvalue("value");

            assert_eq!(key_value_to_string(&key, &value), "key=value");
        }

        #[test]
        fn otlp_format_tags_to_string_test() {
            let attrs = get_sample_dp_attrs();
            let labels = get_sample_resource_attrs();

            assert_eq!(
                tags_to_string(&attrs, &labels),
                "{key1=value1,key2=value2,key3=value3}"
            );
        }

        fn get_sample_number_dp(value: i64, timestamp_ms: u64) -> NumberDataPoint {
            NumberDataPoint {
                attributes: get_sample_dp_attrs(),
                start_time_unix_nano: 161078,
                time_unix_nano: timestamp_ms * NANOS_IN_MILLIS,
                exemplars: vec![],
                flags: 0,
                value: Some(metricsv1::number_data_point::Value::AsInt(value)),
            }
        }

        #[test]
        fn otlp_format_number_datapoint_to_string_test() {
            let name = "metr";
            let attrs = get_sample_resource_attrs();

            let dp = get_sample_number_dp(7312, 2042005);

            assert_eq!(
                number_datapoint_to_line(&dp, name, &attrs),
                "metr{key1=value1,key2=value2,key3=value3} 7312 2042005"
            )
        }

        pub fn get_sample_gauge() -> Gauge {
            Gauge {
                data_points: vec![get_sample_number_dp(78, 1400), get_sample_number_dp(500, 45000)],
            }
        }

        fn get_sample_sum() -> Sum {
            Sum {
                data_points: vec![get_sample_number_dp(78, 1400), get_sample_number_dp(500, 45000)],
                aggregation_temporality: 0,
                is_monotonic: true,
            }
        }

        fn get_sample_metric(name: &str, data: metricsv1::metric::Data) -> Metric {
            Metric {
                name: name.to_string(),
                description: String::new(),
                unit: String::new(),
                data: Some(data),
            }
        }

        #[test]
        fn otlp_format_gauge_to_string_test() {
            let gauge = metricsv1::metric::Data::Gauge(get_sample_gauge());
            let metric = get_sample_metric("eguag", gauge);
            let attrs = get_sample_resource_attrs();

            assert_eq!(
                otlp_metric_to_prometheus_string(&metric, &attrs),
                vec![
                    "eguag{key1=value1,key2=value2,key3=value3} 78 1400",
                    "eguag{key1=value1,key2=value2,key3=value3} 500 45000"
                ],
            )
        }

        #[test]
        fn otlp_format_sum_to_string_test() {
            let sum = metricsv1::metric::Data::Sum(get_sample_sum());
            let metric = get_sample_metric("mus", sum);
            let attrs = get_sample_resource_attrs();

            assert_eq!(
                otlp_metric_to_prometheus_string(&metric, &attrs),
                vec![
                    "mus{key1=value1,key2=value2,key3=value3} 78 1400",
                    "mus{key1=value1,key2=value2,key3=value3} 500 45000"
                ],
            )
        }
    }
}
#[cfg(test)]
mod test {
    use crate::router::otlp::*;
    use actix_http::body::{BoxBody, MessageBody};
    use actix_web::test as actix_test;
    use actix_web::{web, App};
    use bytes::Bytes;
    use opentelemetry_proto::tonic::metrics::v1::{InstrumentationLibraryMetrics, Metric, ResourceMetrics};
    use opentelemetry_proto::tonic::{
        common::v1::{any_value::Value, AnyValue, InstrumentationLibrary, KeyValue},
        logs::v1::{InstrumentationLibraryLogs, LogRecord},
        resource::v1::Resource,
    };

    fn get_default_options() -> options::Options {
        options::Options {
            print: options::Print {
                logs: false,
                headers: false,
                metrics: false,
            },
            delay_time: std::time::Duration::from_secs(0),
            drop_rate: 0,
            store_metrics: true,
            store_logs: true,
        }
    }

    fn get_default_app_data() -> web::Data<AppState> {
        web::Data::new(AppState::new())
    }

    fn get_body_str(body: BoxBody) -> String {
        std::str::from_utf8(&body.try_into_bytes().unwrap())
            .unwrap()
            .to_string()
    }

    fn get_sample_log_record(body: &str) -> LogRecord {
        #[allow(deprecated)]
        LogRecord {
            time_unix_nano: 21,
            observed_time_unix_nano: 99,
            severity_number: 20000,
            severity_text: "warning".to_string(),
            body: Some(AnyValue {
                value: Some(Value::StringValue(body.to_string())),
            }),
            name: "temperature log".to_string(),
            attributes: vec![],
            dropped_attributes_count: 0,
            flags: 0b101010,
            trace_id: vec![],
            span_id: vec![],
        }
    }

    fn get_sample_resource() -> Resource {
        Resource {
            attributes: vec![
                KeyValue {
                    key: "some-key".to_string(),
                    value: Some(AnyValue {
                        value: Some(Value::StringValue("blep".to_string())),
                    }),
                },
                KeyValue {
                    key: "another-key".to_string(),
                    value: Some(AnyValue {
                        value: Some(Value::StringValue("qwerty".to_string())),
                    }),
                },
            ],
            dropped_attributes_count: 0,
        }
    }

    fn get_sample_instr_library() -> InstrumentationLibrary {
        InstrumentationLibrary {
            name: "the best library".to_string(),
            version: "v2.1.5".to_string(),
        }
    }

    fn get_sample_logs_data() -> logsv1::LogsData {
        let resource = get_sample_resource();

        let instr = vec![InstrumentationLibraryLogs {
            instrumentation_library: Some(get_sample_instr_library()),
            log_records: vec![
                get_sample_log_record("warning: the temperature is too low"),
                get_sample_log_record("killing child with a fork"),
            ],
            schema_url: String::new(),
        }];

        let resource_logs_1 = logsv1::ResourceLogs {
            resource: Some(resource.clone()),
            instrumentation_library_logs: instr,
            schema_url: String::new(),
        };

        let resource_logs_2 = resource_logs_1.clone();
        logsv1::LogsData {
            resource_logs: vec![resource_logs_1, resource_logs_2],
        }
    }

    fn get_sample_logs_request_body() -> impl Into<web::Bytes> {
        let logs = get_sample_logs_data();
        logs.encode_to_vec()
    }

    #[test]
    fn otlp_logs_get_metadata_test() {
        let logs = &get_sample_logs_data().resource_logs[0];
        let metadata = get_otlp_metadata_from_logs(logs);

        let mut expected = HashMap::new();
        expected.insert("some-key".to_string(), "blep".to_string());
        expected.insert("another-key".to_string(), "qwerty".to_string());

        assert_eq!(metadata, expected)
    }

    #[test]
    fn otlp_logs_get_lines_test() {
        let logs = &get_sample_logs_data().resource_logs[0];
        let lines = get_otlp_lines_from_logs(logs);

        let expected = vec!["warning: the temperature is too low", "killing child with a fork"];

        assert_eq!(lines, expected)
    }

    #[actix_rt::test]
    async fn otlp_logs_drop_test() {
        let mut opts = get_default_options();
        opts.drop_rate = 100;

        let mut app = actix_test::init_service(
            App::new()
                .app_data(web::Data::new(opts.clone()))
                .app_data(get_default_app_data())
                .service(web::scope("/v1").route("/logs", web::post().to(handler_receiver_otlp_logs)))
                .default_service(web::get().to(handler_receiver)),
        )
        .await;

        {
            // Test logs route.
            let request = actix_test::TestRequest::post()
                .uri("/v1/logs")
                .insert_header(("Content-Type", OTLP_PROTOBUF_FORMAT_CONTENT_TYPE))
                .to_request();

            let response = actix_test::call_service(&mut app, request).await;
            assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
            let body = response.into_body();
            assert_eq!(
                get_body_str(body),
                format!("Dropping data for {}", OTLP_PROTOBUF_FORMAT_CONTENT_TYPE)
            );
        }

        {
            // Test metrics route.
            let request = actix_test::TestRequest::post()
                .uri("/v1/metrics")
                .insert_header(("Content-Type", OTLP_PROTOBUF_FORMAT_CONTENT_TYPE))
                .to_request();

            let response = actix_test::call_service(&mut app, request).await;
            assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
            let body = response.into_body();
            assert_eq!(
                get_body_str(body),
                format!("Dropping data for {}", OTLP_PROTOBUF_FORMAT_CONTENT_TYPE)
            );
        }
    }

    #[actix_rt::test]
    async fn otlp_unrelated_content_type_test() {
        let content_type = "unrelated/type";
        let opts = get_default_options();
        let mut app = actix_test::init_service(
            App::new()
                .app_data(web::Data::new(opts.clone()))
                .app_data(get_default_app_data())
                .service(
                    web::scope("/v1")
                        .route("/logs", web::post().to(handler_receiver_otlp_logs))
                        .route("/metrics", web::post().to(handler_receiver_otlp_metrics)),
                )
                .default_service(web::get().to(handler_receiver)),
        )
        .await;
        {
            // Test logs route.
            let request = actix_test::TestRequest::post()
                .uri("/v1/logs")
                .insert_header(("Content-Type", content_type))
                .to_request();

            let response = actix_test::call_service(&mut app, request).await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        }

        {
            // Test metrics route.
            let request = actix_test::TestRequest::post()
                .uri("/v1/metrics")
                .insert_header(("Content-Type", content_type))
                .to_request();

            let response = actix_test::call_service(&mut app, request).await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        }
    }

    #[actix_rt::test]
    async fn otlp_logs_store_test() {
        let opts = get_default_options();
        let mut app = actix_test::init_service(
            App::new()
                .app_data(web::Data::new(opts.clone()))
                .app_data(get_default_app_data())
                .service(web::scope("/v1").route("/logs", web::post().to(handler_receiver_otlp_logs)))
                .route("/logs/count", web::get().to(handler_logs_count))
                .default_service(web::get().to(handler_receiver)),
        )
        .await;

        {
            let request = actix_test::TestRequest::post()
                .uri("/v1/logs")
                .insert_header(("Content-Type", OTLP_PROTOBUF_FORMAT_CONTENT_TYPE))
                .set_payload(get_sample_logs_request_body())
                .to_request();

            let response = actix_test::call_service(&mut app, request).await;
            assert_eq!(response.status(), StatusCode::OK);
        }

        // count all the logs
        {
            let req = actix_test::TestRequest::get().uri("/logs/count").to_request();
            let resp = actix_test::call_service(&mut app, req).await;

            let response_body: LogsCountResponse = actix_test::read_body_json(resp).await;

            assert_eq!(response_body.count, 4);
        }
    }

    fn get_sample_metric(name: &str) -> Metric {
        Metric {
            name: name.to_string(),
            description: "a test metric".to_string(),
            unit: "petaweber".to_string(),
            data: Some(metricsv1::metric::Data::Gauge(format::test::get_sample_gauge())),
        }
    }

    fn get_sample_metrics_data() -> metricsv1::MetricsData {
        let resource = get_sample_resource();
        let instr = vec![InstrumentationLibraryMetrics {
            instrumentation_library: Some(get_sample_instr_library()),
            metrics: vec![get_sample_metric("length"), get_sample_metric("breath")],
            schema_url: "".to_string(),
        }];
        let resource_metrics_1 = ResourceMetrics {
            resource: Some(resource),
            instrumentation_library_metrics: instr,
            schema_url: "".to_string(),
        };
        let resource_metrics_2 = resource_metrics_1.clone();

        metricsv1::MetricsData {
            resource_metrics: vec![resource_metrics_1, resource_metrics_2],
        }
    }

    fn get_sample_metrics_request_body() -> impl Into<web::Bytes> {
        let metrics = get_sample_metrics_data();
        metrics.encode_to_vec()
    }

    #[actix_rt::test]
    async fn otlp_metrics_store_test() {
        let opts = get_default_options();
        let mut app = actix_test::init_service(
            App::new()
                .app_data(web::Data::new(opts.clone()))
                .app_data(get_default_app_data())
                .service(web::scope("/v1").route("/metrics", web::post().to(handler_receiver_otlp_metrics)))
                .route("/metrics-list", web::get().to(handler_metrics_list))
                .default_service(web::get().to(handler_receiver)),
        )
        .await;

        {
            let request = actix_test::TestRequest::post()
                .uri("/v1/metrics")
                .insert_header(("Content-Type", OTLP_PROTOBUF_FORMAT_CONTENT_TYPE))
                .set_payload(get_sample_metrics_request_body())
                .to_request();

            let response = actix_test::call_service(&mut app, request).await;
            assert_eq!(response.status(), StatusCode::OK);
        }

        {
            let request = actix_test::TestRequest::get().uri("/metrics-list").to_request();

            let response = actix_test::call_service(&mut app, request).await;
            assert_eq!(response.status(), StatusCode::OK);

            let body = actix_test::read_body(response).await;

            assert!(body.eq(&Bytes::from("length: 2\nbreath: 2\n")) || body.eq(&Bytes::from("breath: 2\nlength: 2\n")));
        }
    }

    #[actix_rt::test]
    async fn otlp_metrics_store_samples_test() {
        let opts = get_default_options();
        let mut app = actix_test::init_service(
            App::new()
                .app_data(web::Data::new(opts.clone()))
                .app_data(get_default_app_data())
                .service(web::scope("/v1").route("/metrics", web::post().to(handler_receiver_otlp_metrics)))
                .route("/metrics-samples", web::get().to(handler_metrics_samples))
                .default_service(web::get().to(handler_receiver)),
        )
        .await;

        {
            let request = actix_test::TestRequest::post()
                .uri("/v1/metrics")
                .insert_header(("Content-Type", OTLP_PROTOBUF_FORMAT_CONTENT_TYPE))
                .set_payload(get_sample_metrics_request_body())
                .to_request();

            let response = actix_test::call_service(&mut app, request).await;
            assert_eq!(response.status(), StatusCode::OK);
        }

        {
            let request = actix_test::TestRequest::get()
                .uri("/metrics-samples")
                .set_payload(get_sample_metrics_request_body())
                .to_request();
            let response = actix_test::call_service(&mut app, request).await;

            assert_eq!(response.status(), StatusCode::OK);

            let result: Vec<Sample> = actix_test::read_body_json(response).await;
            assert_eq!(result.len(), 2);
        }
    }
}
