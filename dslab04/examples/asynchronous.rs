// I/O-bound tasks, like downloading websites, usually do not require a lot of
// CPU computations. Therefore, it might be more efficient to implement them as
// asynchronous tasks.
//
// These examples present the ideas behind asynchronous programming: asynchronously
// downloading multiple websites. The implementation details are omitted here on
// purpose, as they will be discussed in details in further examples.

// To run multiple futures asynchronously, we can join them into a single future
// and then await it:
#[tokio::main]
async fn example_1() {
    // Asynchronously, all...
    let responses = futures::future::join_all(
        // ...3 websites...
        vec![
            "https://google.com",
            "https://duckduckgo.com",
            "https://www.bing.com",
        ]
        .into_iter()
        // ...are downloaded:
        .map(reqwest::get),
    )
    // We wait until all downloads are completed (which should
    // take as much time as the longest download),...
    .await
    // ...and we collect the responses into a vector:
    .into_iter()
    .flatten()
    .collect::<Vec<reqwest::Response>>();

    // Print the responses:
    println!("{responses:#?}");
}

// Another way to run multiple futures asynchronously is to spawn each of them
// in a separate task and then await the tasks' completion:
#[tokio::main]
async fn example_2() {
    let urls = vec![
        "https://http.dog/200.jpg",
        "https://picsum.photos/4200/5600",
        "https://google.com",
        "https://duckduckgo.com",
        "https://www.bing.com",
    ];

    let client = reqwest::Client::new();

    let mut tasks = vec![];
    // For each website...
    for url in urls {
        let client_cloned = client.clone();
        tasks.push(
            // ...run an asychronous task...
            tokio::spawn(async move {
                // ...which will download it:
                client_cloned.get(url).send().await.unwrap()
            }),
        );
    }

    // The websites are asynchronously downloaded now.

    let mut responses = vec![];
    // For each task...
    for task in tasks {
        // ...collect the response...
        responses.push(
            // ...when the task is completed:
            task.await.unwrap(),
            // I think we do that well after a task is completed
            // since we await a task only when we already awaited previous ones
            // but I guess you could poll this or sth
        );
    }

    // Print the responses:
    println!("{responses:#?}");
}

#[tokio::main]
async fn example_my() {
    let client = reqwest::Client::new();

    for _ in 1..100 {
        let client_clone = client.clone();

        tokio::spawn(async move {
            client_clone.get("https://picsum.photos/4200/5600").send().await.unwrap();
        }).await.unwrap(); // this is a bad example, no real asynchrony here
        // since we want to await each task before spawning the next
    }
}
// The examples require an Internet access to work:
fn main() {
    // example_1();
    // example_2();

    example_my();
}
