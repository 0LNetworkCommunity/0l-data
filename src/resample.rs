pub fn resample(start: u64, end: u64, interval: u64, series: &Vec<(u64, f64)>) -> Vec<(u64, f64)> {
    let output_len = (end - start) / interval + 1;
    let mut ouput = Vec::with_capacity(output_len as usize);

    let mut j = 0;

    let mut value = 0f64;
    let series_len = series.len();

    for i in 0..output_len {
        let time = start + interval * i;

        while j < series_len && series[j].0 <= time {
            value = series[j].1;
            j += 1;
        }
        ouput.push((time, value));
    }

    ouput
}
