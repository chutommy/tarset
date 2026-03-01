use std::path::Path;
use tapeset::reader::SampleReader;
use tapeset::resolve;
use tapeset::writer::SampleWriter;

fn main() {
    // let sources = vec![
    //     "/capstor/store/cscs/swissai/infra01/vision-datasets/RSTeller",
    //     "/capstor/store/cscs/swissai/infra01/vision-datasets/medical/apertus_image_text_v1/iu_xray",
    //     "/capstor/store/cscs/swissai/infra01/vision-datasets/medical/apertus_image_text_v1/covid_us",
    //     "/capstor/store/cscs/swissai/infra01/vision-datasets/medical/apertus_image_text_v1/brain_tumor_mri",
    //     "/capstor/store/cscs/swissai/infra01/vision-datasets/medical/apertus_image_text_v1/nih_chest_xray",
    // ];
    let sources = vec!["./data/src"];

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

        let dest = Path::new("./data/dest/output.tar.gz");
        let mut writer = match SampleWriter::create(dest) {
            Ok(w) => w,
            Err(e) => {
                println!("Error creating writer for {}: {e:#}", dest.display());
                continue;
            }
        };

        // let reader = reader.with_suffixes(vec!["png".to_string()]);

        for sample in reader {
            match sample {
                Ok(sample) => {
                    let suffixes: Vec<&str> =
                        sample.fields.iter().map(|f| f.suffix.as_str()).collect();
                    writer.write_sample(&sample).unwrap_or_else(|e| {
                        eprintln!("Error writing sample: {e:#}");
                    });
                    println!("  key={} suffixes={suffixes:?}", sample.key);
                }
                Err(e) => eprintln!("Error reading sample: {e:#}"),
            }
        }
    }
}
