extern crate glob;
extern crate rayon;
extern crate redis;
extern crate regex;
extern crate serde_json;
#[macro_use] extern crate serde_derive;
extern crate clap;

use clap::{Arg, App, SubCommand};
use glob::glob;
use rayon::prelude::*;
use rayon::iter::Map as RayonMap;
use redis::Commands;
use regex::Captures;
use regex::Regex;
use serde_json::{Value, Error};
use std::cell::RefCell;
use std::fs::File;
use std::io::prelude::*;
use std::time::Instant;
use std::path::PathBuf;
use std::iter::Map;
use glob::GlobResult;
use rayon::iter::plumbing::Producer;
use std::cmp::min;
use std::str::from_utf8_unchecked;


const  CHARS_PER_DOC: usize = 200;

struct Case {
    path: String,
    contents: String,
}

#[derive(Deserialize)]
struct Job {
    id: String,
    re: String,
}

fn elapsed_time(instant: Instant) -> f64{
    let elapsed = instant.elapsed();
    elapsed.as_secs() as f64 + elapsed.subsec_nanos() as f64 * 1e-9
}

fn get_paths(path:&str) -> Vec<String>{
    glob(path)
        .expect("Failed to read glob pattern")
        .map(|path| path.expect("globbing failed").into_os_string().into_string().expect("OS string isn't a valid string"))
        .collect()
}

fn get_file(path:&String) -> String{
    let mut f = File::open(&path).expect("file not found");
    let mut contents = String::new();
    f.read_to_string(&mut contents).expect("something went wrong reading file");
    contents
}

fn get_matches<'a>(contents:&'a str, re:&Regex) -> Vec<&'a str> {
    let mut results:Vec<&str> = Vec::new();
    let mut remaining_len = CHARS_PER_DOC;
    for cap in re.captures_iter(&contents){
        let m = cap.get(0).unwrap().as_str();
        let take = min(remaining_len, m.len());
        results.push(&m[0..take]);
        remaining_len -= take;
        if remaining_len == 0 { break };
    }
    results
}

fn main() {
    // parse command line args
    let matches = App::new("grep_caselaw")
        .arg(Arg::with_name("path_glob")
            .short("g")
            .long("glob")
            .value_name("PATH")
            .help("Glob to load files, e.g. 'data/illinois/ill_text/**/*.txt'")
            .takes_value(true))
        .arg(Arg::with_name("test")
            .short("t")
            .long("test")
            .help("Run repeated test query"))
        .get_matches();
    let default_path = "data/illinois/ill_text/**/*.txt";
    let default_path = "data/illinois/ill_text/Ill. Ct. Cl./**/*.txt";
    let path = matches.value_of("dir").unwrap_or(default_path);
    let run_tests = matches.is_present("test");

    // set up threadpool so each thread has its own redis connection
    thread_local! {
        static CON : RefCell<Option<redis::Connection>> = RefCell::new(None);
    }
    let threadpool = rayon::ThreadPoolBuilder::new()
        .start_handler(|_|{
            let client = redis::Client::open("redis://127.0.0.1/5").expect("Error parsing redis connection string");
            CON.with(|con_cell|{
                let con = client.get_connection().expect("Error connecting to redis");
                *con_cell.borrow_mut() = Some(con);
            });
        })
        .build_global().unwrap();

    // set up global redis connection
    let client: redis::Client = redis::Client::open("redis://127.0.0.1/5").expect("Error parsing redis connection string");
    let con = client.get_connection().expect("Error connecting to redis");

    // load caselaw into ram
    println!("- Loading caselaw data ...");
    let now = Instant::now();
    let files = get_paths(path)
        .par_iter()
        .map(get_file)
        .collect::<Vec<String>>();
    println!(" - Loaded {} cases in {} seconds.", files.len(), elapsed_time(now));

    let mut times = Vec::new();
    if run_tests{
        let _ : () = con.lpush("queue", "{\"id\":\"1\", \"re\":\"([dD])efendant|(Pp)laintiff\"}").expect("Couldn't queue job");
    }

    loop {
        // wait until we get the next job
        // 0 means wait forever
        let job_json:Vec<String> = con.brpop("queue", 0).expect("Error fetching next job");

        // parse job
        println!("Got job: {:?}", job_json);
        let job: Job = serde_json::from_str(&job_json[1]).expect("Failed to parse job json");
        let re = Regex::new(&job.re).expect("Error parsing regular expression");

        println!("- Searching");
        let now = Instant::now();
        let match_count:usize = files
            .par_chunks(1000)
            .map(|chunk|{
                let re_local = re.clone();

                let mut matches = Vec::new();
                for file in chunk{
                    let file_matches = get_matches(file, &re_local);
                    if file_matches.len() > 0 {
                        matches.push(file_matches);
                    }
                }

                CON.with(|con_cell|{
                    match *con_cell.borrow(){
                        Some(ref con) => {
                            let _ : () = con.lpush(&job.id, serde_json::to_string(&matches).expect("Error serializing matches")).expect("Couldn't add results to queue");
                        },
                        None => {
                            panic!("no redis connection");
                        }
                    }
                });

                matches.len()
            })
            .sum();
        println!(" - Found {} matching files in {} seconds", match_count, elapsed_time(now));

        if run_tests {
            times.push(elapsed_time(now));
            println!("- Average time {}", times.iter().sum::<f64>() / times.len() as f64);
            let _: () = con.del("1").expect("Couldn't delete key");
            let _: () = con.lpush("queue", "{\"id\":\"1\", \"re\":\"([dD])efendant|(Pp)laintiff\"}").expect("Couldn't queue job");
        }
    }
}
