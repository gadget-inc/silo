#!/usr/bin/env node
/**
 * Validates that all SILO-* tags found in the Alloy model exist in the Rust
 * implementation, and vice versa.
 *
 * Usage: node scripts/validate-silo-tags.js
 */

const fs = require("fs");
const path = require("path");

const ALLOY_SPEC = "specs/job_shard.als";
const RUST_SRC_DIR = "src";

// Regex to match SILO-* tags like [SILO-ENQ-1], [SILO-DEQ-CXL-REL], etc.
const SILO_TAG_REGEX = /\[SILO-[A-Z0-9-]+\]/g;

/**
 * Recursively find all files with a given extension in a directory.
 */
function findFiles(dir, ext, results = []) {
    const entries = fs.readdirSync(dir, { withFileTypes: true });
    for (const entry of entries) {
        const fullPath = path.join(dir, entry.name);
        if (entry.isDirectory()) {
            findFiles(fullPath, ext, results);
        } else if (entry.isFile() && entry.name.endsWith(ext)) {
            results.push(fullPath);
        }
    }
    return results;
}

/**
 * Extract all unique SILO-* tags from a file's contents.
 */
function extractTags(content) {
    const matches = content.match(SILO_TAG_REGEX) || [];
    return new Set(matches);
}

/**
 * Extract tags from multiple files.
 */
function extractTagsFromFiles(files) {
    const allTags = new Set();
    const tagLocations = new Map(); // tag -> [files]

    for (const file of files) {
        const content = fs.readFileSync(file, "utf-8");
        const tags = extractTags(content);
        for (const tag of tags) {
            allTags.add(tag);
            if (!tagLocations.has(tag)) {
                tagLocations.set(tag, []);
            }
            tagLocations.get(tag).push(file);
        }
    }

    return { tags: allTags, locations: tagLocations };
}

function main() {
    // Find project root (look for Cargo.toml)
    let projectRoot = process.cwd();
    while (!fs.existsSync(path.join(projectRoot, "Cargo.toml"))) {
        const parent = path.dirname(projectRoot);
        if (parent === projectRoot) {
            console.error("Error: Could not find project root (no Cargo.toml found)");
            process.exit(1);
        }
        projectRoot = parent;
    }

    const alloySpecPath = path.join(projectRoot, ALLOY_SPEC);
    const rustSrcDir = path.join(projectRoot, RUST_SRC_DIR);

    // Check that files/dirs exist
    if (!fs.existsSync(alloySpecPath)) {
        console.error(`Error: Alloy spec not found at ${alloySpecPath}`);
        process.exit(1);
    }
    if (!fs.existsSync(rustSrcDir)) {
        console.error(`Error: Rust src directory not found at ${rustSrcDir}`);
        process.exit(1);
    }

    // Extract tags from Alloy spec
    const alloyContent = fs.readFileSync(alloySpecPath, "utf-8");
    const alloyTags = extractTags(alloyContent);

    // Extract tags from Rust implementation (excluding tests)
    const rustFiles = findFiles(rustSrcDir, ".rs");
    const { tags: rustTags, locations: rustLocations } =
        extractTagsFromFiles(rustFiles);

    // Find tags only in Alloy (missing from Rust)
    const onlyInAlloy = [...alloyTags].filter((tag) => !rustTags.has(tag)).sort();

    // Find tags only in Rust (missing from Alloy)
    const onlyInRust = [...rustTags].filter((tag) => !alloyTags.has(tag)).sort();

    // Find tags in both
    const inBoth = [...alloyTags].filter((tag) => rustTags.has(tag)).sort();

    // Report results
    console.log("SILO Tag Validation Report");
    console.log("==========================\n");

    console.log(`Alloy spec: ${ALLOY_SPEC}`);
    console.log(`Rust source: ${RUST_SRC_DIR}/\n`);

    console.log(`Tags found in both: ${inBoth.length}`);
    console.log(`Tags only in Alloy: ${onlyInAlloy.length}`);
    console.log(`Tags only in Rust:  ${onlyInRust.length}\n`);

    let hasErrors = false;

    if (onlyInAlloy.length > 0) {
        hasErrors = true;
        console.log("❌ Tags in Alloy model but MISSING from Rust implementation:");
        for (const tag of onlyInAlloy) {
            console.log(`   ${tag}`);
        }
        console.log();
    }

    if (onlyInRust.length > 0) {
        hasErrors = true;
        console.log("❌ Tags in Rust implementation but MISSING from Alloy model:");
        for (const tag of onlyInRust) {
            const files = rustLocations.get(tag) || [];
            const fileList = files.map((f) => path.relative(projectRoot, f));
            console.log(`   ${tag} (in: ${fileList.join(", ")})`);
        }
        console.log();
    }

    if (!hasErrors) {
        console.log("✅ All SILO tags are synchronized between Alloy and Rust!\n");

        // Optionally list all tags
        if (process.argv.includes("--verbose") || process.argv.includes("-v")) {
            console.log("Tags:");
            for (const tag of inBoth) {
                console.log(`   ${tag}`);
            }
        }
    }

    process.exit(hasErrors ? 1 : 0);
}

main();
