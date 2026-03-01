use tapeset::reader::SampleReader;
use tapeset::resolve;

fn main() {
    // let sources = vec![
    //     "/capstor/store/cscs/swissai/infra01/vision-datasets/RSTeller",
    //     "/capstor/store/cscs/swissai/infra01/vision-datasets/medical/apertus_image_text_v1/iu_xray",
    //     "/capstor/store/cscs/swissai/infra01/vision-datasets/medical/apertus_image_text_v1/covid_us",
    //     "/capstor/store/cscs/swissai/infra01/vision-datasets/medical/apertus_image_text_v1/brain_tumor_mri",
    //     "/capstor/store/cscs/swissai/infra01/vision-datasets/medical/apertus_image_text_v1/nih_chest_xray",
    // ];
    let sources = vec!["./"];

    let resolved = match resolve::resolve_sources(&sources) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Error resolving sources: {e:#}");
            return;
        }
    };

    println!("Resolved {} tar files:", resolved.len());
    for path in &resolved {
        println!("  {}", path.display());
    }

    for path in &resolved {
        let reader = match SampleReader::open(path) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("Error opening {}: {e:#}", path.display());
                continue;
            }
        };

        // let reader = reader.with_suffixes(vec!["png".to_string()]);

        println!("\nSamples in {}:", path.display());
        let mut count = 0;
        for sample in reader {
            match sample {
                Ok(sample) => {
                    let suffixes: Vec<&str> =
                        sample.fields.iter().map(|f| f.suffix.as_str()).collect();
                    count += 1;
                    println!("  key={} suffixes={suffixes:?}", sample.key);
                    if count >= 5 {
                        println!("  ...");
                        break;
                    }
                }
                Err(e) => eprintln!("Error reading sample: {e:#}"),
            }
        }
    }
}
