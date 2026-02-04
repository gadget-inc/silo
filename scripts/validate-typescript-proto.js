#!/usr/bin/env node
/**
 * Validates that the TypeScript client's generated proto files are up to date
 * with the silo.proto source file.
 *
 * Usage: node scripts/validate-typescript-proto.js
 */

const fs = require("fs");
const path = require("path");
const { execSync } = require("child_process");
const os = require("os");

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

    const tsClientDir = path.join(projectRoot, "typescript_client");
    const pbDir = path.join(tsClientDir, "src", "pb");
    const protoFile = path.join(projectRoot, "proto", "silo.proto");

    // Verify required files exist
    if (!fs.existsSync(protoFile)) {
        console.error(`Error: Proto file not found at ${protoFile}`);
        process.exit(1);
    }
    if (!fs.existsSync(pbDir)) {
        console.error(`Error: Generated proto directory not found at ${pbDir}`);
        process.exit(1);
    }

    // Create a temporary directory for regenerating
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "silo-proto-check-"));
    const tmpPbDir = path.join(tmpDir, "pb");
    fs.mkdirSync(tmpPbDir);

    try {
        console.log("TypeScript Proto Validation");
        console.log("===========================\n");

        console.log(`Proto source: proto/silo.proto`);
        console.log(`Generated files: typescript_client/src/pb/\n`);

        // Regenerate proto files to temp directory
        console.log("Regenerating proto files...\n");
        const protocCmd = `pnpm exec protoc --ts_out=${tmpPbDir} --ts_opt=long_type_bigint --ts_opt=generate_dependencies --proto_path=${path.join(projectRoot, "proto")} ${protoFile}`;

        try {
            execSync(protocCmd, { cwd: tsClientDir, stdio: "pipe" });
        } catch (err) {
            console.error("Error running protoc:");
            console.error(err.stderr?.toString() || err.message);
            process.exit(1);
        }

        // Compare generated files
        const existingFiles = fs.readdirSync(pbDir).filter(f => f.endsWith(".ts")).sort();
        const newFiles = fs.readdirSync(tmpPbDir).filter(f => f.endsWith(".ts")).sort();

        // Check for file list differences
        const existingSet = new Set(existingFiles);
        const newSet = new Set(newFiles);

        const missingFiles = newFiles.filter(f => !existingSet.has(f));
        const extraFiles = existingFiles.filter(f => !newSet.has(f));

        let hasErrors = false;

        if (missingFiles.length > 0) {
            hasErrors = true;
            console.log("Missing generated files:");
            for (const f of missingFiles) {
                console.log(`   ${f}`);
            }
            console.log();
        }

        if (extraFiles.length > 0) {
            hasErrors = true;
            console.log("Extra files (not in fresh generation):");
            for (const f of extraFiles) {
                console.log(`   ${f}`);
            }
            console.log();
        }

        // Compare file contents
        const filesToCompare = existingFiles.filter(f => newSet.has(f));
        const differentFiles = [];

        for (const file of filesToCompare) {
            const existingContent = fs.readFileSync(path.join(pbDir, file), "utf-8");
            const newContent = fs.readFileSync(path.join(tmpPbDir, file), "utf-8");

            if (existingContent !== newContent) {
                differentFiles.push(file);
            }
        }

        if (differentFiles.length > 0) {
            hasErrors = true;
            console.log("Files with outdated content:");
            for (const f of differentFiles) {
                console.log(`   typescript_client/src/pb/${f}`);
            }
            console.log();
        }

        if (hasErrors) {
            console.log("❌ TypeScript proto files are out of date!");
            console.log("\nRun the following to fix:");
            console.log("   cd typescript_client && pnpm run build\n");
            process.exit(1);
        } else {
            console.log("✅ TypeScript proto files are up to date!\n");
        }
    } finally {
        // Clean up temp directory
        fs.rmSync(tmpDir, { recursive: true, force: true });
    }
}

main();
