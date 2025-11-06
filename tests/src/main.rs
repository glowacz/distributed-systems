fn main() {
    // let data = vec![
    //     vec![
    //         vec![111, 112, 113, 114], 
    //         vec![121, 122]
    //     ], 
    //     vec![
    //         vec![211, 212],
    //         vec![221, 222]
    //     ]
    // ];
    let data = vec![vec![1, 2, 3, 4], vec![5, 6]];
    let flattened = data.into_iter().flatten();
    // let flattened = data.into_iter().flatten().collect();
    
    for val in flattened.clone() {
        println!("{:?}", val)
    }

    // bez clone wcześniej byśmy skonsumowali nasz główny wektor (movneli każdy element)
    for val in flattened {
        println!("{:?}", val)
    }

    // println!("{:?}", flattened);
}