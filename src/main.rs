use evtx::EvtxRecord;
use parquet::basic::{Compression, LogicalType, Type, TypePtr};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::{TryFileWriter, SerializedFileWriter};
use parquet::schema::types::SchemaDescriptor;
use std::sync::Arc;
use std::io;
use std::path::PathBuf;
use std::thread;
use std::time::Duration;

fn main() -> io::Result<()> {
    // Define log types
    let event_types = vec!["Application", "Security", "Setup", "System"];

    // Output file folder
    let log_output_folder = PathBuf::from("C:\\data\\EventLogs");

    loop {
        for event_type in &event_types {
            // Build output file path
            let output_file = log_output_folder.join(format!("{}.parquet", event_type));

            // Get latest events
            if let Ok(events) = get_latest_events(event_type, 10) {
                // Export events to Parquet
                if let Err(e) = export_events_to_parquet(&events, &output_file) {
                    eprintln!("Error exporting events to Parquet: {}", e);
                }
            } else {
                eprintln!("Error fetching events for {}", event_type);
            }
        }

        // Sleep for 60 seconds
        thread::sleep(Duration::from_secs(60));
    }
}

fn get_latest_events(event_type: &str, max_events: usize) -> io::Result<Vec<EvtxRecord>> {
    let log_file_path = format!("C:\\Windows\\System32\\winevt\\Logs\\{}.evtx", event_type);

    let mut parser = evtx::Parser::from_path(log_file_path)?;
    let mut records = Vec::new();

    // Read and collect the latest events
    for record in parser.records().take(max_events) {
        records.push(record?);
    }

    Ok(records)
}

fn export_events_to_parquet(events: &[EvtxRecord], output_file: &PathBuf) -> io::Result<()> {
    let mut schema_fields = Vec::new();
    schema_fields.push(TypePtr::new_basic_type("TimeCreated", LogicalType::TIMESTAMP_MICROS));
    schema_fields.push(TypePtr::new_basic_type("Id", LogicalType::INT_32));
    schema_fields.push(TypePtr::new_basic_type("ProviderName", LogicalType::UTF8));
    schema_fields.push(TypePtr::new_basic_type("Message", LogicalType::UTF8));

    let schema = Arc::new(SchemaDescriptor::new(schema_fields));

    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();

    let mut writer = SerializedFileWriter::new(output_file.clone(), schema, props)?;

    for event in events {
        let mut row_group_writer = writer.next_row_group()?;

        let mut col_writers = Vec::new();
        for i in 0..schema.num_columns() {
            let column_writer = row_group_writer.next_column()?;
            col_writers.push(column_writer);
        }

        for (i, writer) in col_writers.iter_mut().enumerate() {
            match i {
                0 => writer.write_batch(&vec![event.system_time() as i64])?,
                1 => writer.write_batch(&vec![event.event_id() as i32])?,
                2 => writer.write_batch(&vec![event.provider_name().to_string()])?,
                3 => writer.write_batch(&vec![event.rendered_message().unwrap_or_default()])?,
                _ => unreachable!(),
            }
        }

        row_group_writer.close()?;
    }

    writer.close()?;
    Ok(())
}
