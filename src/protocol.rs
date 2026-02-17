// Request/response parsing

#[derive(Debug)]
pub enum Command {
    Set { key: String, value: String },
    Get { key: String },
    Delete { key: String },
}

pub fn parse_line(line: &str) -> Result<Command, String>{

    let parts: Vec<&str> = line.trim().split_whitespace().collect();

    if parts.is_empty() {
        return Err("Empty command".into());
    }

    match parts[0].to_uppercase().as_str() {
        
        "SET" => {

            if parts.len() != 3 {
                return Err("SET requires key and value".into());
            }

            Ok(Command::Set { 
                key: parts[1].to_string(),
                value: parts[2].to_string()
            })
        }

        "GET" => {

            if parts.len() != 2{
                 return Err("GET requires key".into());
            }

            Ok(Command::Get {
                key: parts[1].to_string()
            })
        }
        
        "DELETE" | "DEL" => {
            if parts.len() != 2 {
                return Err("DELETE requires key".into());
            }

            Ok(Command::Delete {
                key: parts[1].to_string(),
            })
        }

         _ => Err("Unknown command".into()),
    }

    
}
