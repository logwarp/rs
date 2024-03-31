use std::process::Command;
use std::thread;
use std::time::Duration;

fn main() {
    // Define PowerShell script
    let powershell_script = r#"
        # Set script start time
        $ScriptStartTime = Get-Date

        # Define log types 
        $EventTypes = @('Application', 'Security', 'Setup', 'System')

        # Output file folder
        $LogOutputFolder = 'C:\\data\\EventLogs'

        # Start infinite loop
        while ($true) {

          # Loop through each log type
          foreach ($EventType in $EventTypes) {

            # Build output file path 
            $OutputFile = Join-Path $LogOutputFolder "$EventType.csv"

            # Get latest events
            $events = Get-WinEvent -LogName $EventType -MaxEvents 10

            # Select properties
            $events = $events | Select-Object TimeCreated, Id, ProviderName, Message

            # Export to CSV
            $events | Export-Csv -Path $OutputFile -Append -NoTypeInformation

          }  

          # Sleep for 60 seconds
          Start-Sleep -Seconds 60

        }

        # Calculate script duration
        $ScriptEndTime = Get-Date
        $ScriptDuration = New-Timespan -Start $ScriptStartTime -End $ScriptEndTime

        Write-Output "Script duration: $ScriptDuration"
    "#;

    // Start infinite loop
    loop {
        // Execute PowerShell script
        let output = Command::new("powershell")
            .args(&["-Command", powershell_script])
            .output()
            .expect("Failed to execute PowerShell script");

        if !output.status.success() {
            eprintln!("Error: {:?}", output.stderr);
        }

        // Sleep for 60 seconds
        thread::sleep(Duration::from_secs(60));
    }
}
