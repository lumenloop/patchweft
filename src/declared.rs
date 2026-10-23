//! Declared inputs (`patchweft.inputs/1`): a project's own statement, in its tree, of the files
//! each CI job reads. A declared job is keyed on those files instead of the whole commit.
//! Anything absent, invalid or undeclarable keeps today's commit key (the fail-safe path).

use crate::{
    model::{Profile, Reuse},
    source,
};
use anyhow::{Context, Result, anyhow, bail, ensure};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

pub const SCHEMA: &str = "patchweft.inputs/1";

/// A pattern of the glob dialect: anchored at the repository root, `/`-separated and
/// case-sensitive. `*` and `?` stay within a segment; `**` is a whole segment.
#[derive(Debug, Clone)]
pub struct Glob(Vec<Segment>);

#[derive(Debug, Clone, PartialEq)]
enum Segment {
    /// `**`: zero or more whole segments.
    Any,
    Name(Vec<char>),
}

impl Glob {
    pub fn parse(pattern: &str) -> Result<Self> {
        let refuse = || anyhow!("pattern outside the glob dialect: {pattern}");
        if pattern.starts_with('!') || pattern.contains(['{', '}', '[', ']', '\\']) {
            return Err(refuse());
        }
        let mut segments = Vec::new();
        // An empty segment covers the empty pattern, a leading `/`, a trailing `/` and `//`.
        for segment in pattern.split('/') {
            let segment = match segment {
                "" => return Err(refuse()),
                "**" => Segment::Any,
                _ if segment.contains("**") => return Err(refuse()),
                _ => Segment::Name(segment.chars().collect()),
            };
            // Adjacent `**` mean the same as one, without the backtracking.
            if !(segment == Segment::Any && segments.last() == Some(&Segment::Any)) {
                segments.push(segment);
            }
        }
        Ok(Self(segments))
    }

    pub fn matches(&self, path: &str) -> bool {
        let path: Vec<&str> = path.split('/').collect();
        segments_match(&self.0, &path)
    }
}

fn segments_match(pattern: &[Segment], path: &[&str]) -> bool {
    match pattern.split_first() {
        None => path.is_empty(),
        Some((Segment::Any, rest)) => {
            (0..=path.len()).any(|skip| segments_match(rest, &path[skip..]))
        }
        Some((Segment::Name(name), rest)) => path
            .split_first()
            .is_some_and(|(first, tail)| name_matches(name, first) && segments_match(rest, tail)),
    }
}

/// `*` matches any run of characters, a leading dot included; `?` matches exactly one.
fn name_matches(pattern: &[char], name: &str) -> bool {
    let name: Vec<char> = name.chars().collect();
    let (mut p, mut n) = (0, 0);
    // The latest `*` and the name position it currently extends to.
    let mut star = None;
    while n < name.len() {
        match pattern.get(p) {
            Some('*') => {
                star = Some((p, n));
                p += 1;
            }
            Some(&c) if c == '?' || c == name[n] => {
                p += 1;
                n += 1;
            }
            _ => match star {
                Some((at, from)) => {
                    star = Some((at, from + 1));
                    p = at + 1;
                    n = from + 1;
                }
                None => return false,
            },
        }
    }
    pattern[p..].iter().all(|c| *c == '*')
}

/// One tracked file of `git ls-tree -r`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tracked {
    pub path: String,
    pub mode: String,
    pub oid: String,
}

impl Tracked {
    /// A symlink or gitlink: its content is not the bytes a job reads.
    fn link(&self) -> bool {
        ["120000", "160000"].contains(&self.mode.as_str())
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct File {
    /// Checked before this parse, so that another schema is reported as such.
    #[allow(dead_code)]
    schema: String,
    #[serde(default)]
    common: Vec<String>,
    #[serde(default)]
    jobs: BTreeMap<String, Map<String, Value>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    paths: Option<Vec<String>>,
    #[serde(default)]
    exclude: Vec<String>,
    #[serde(default)]
    keep: Vec<String>,
    #[serde(default)]
    base_paths: Vec<String>,
    /// The reason the job reads the whole tree; such a job keeps the commit key.
    whole_tree: Option<String>,
    why: Option<String>,
}

/// A parsed declarations file; its patterns are all within the dialect.
#[derive(Debug)]
pub struct Declarations {
    common: Vec<String>,
    /// Each entry with its JSON as written, the source of its identity.
    entries: BTreeMap<String, (Entry, Map<String, Value>)>,
}

impl Declarations {
    /// The exact key, else the longest family (`prefix*`) covering the job.
    fn entry(&self, job: &str) -> Option<&(Entry, Map<String, Value>)> {
        self.entries.get(job).or_else(|| {
            self.entries
                .iter()
                .filter(|(key, _)| family(key, job))
                .max_by_key(|(key, _)| key.len())
                .map(|(_, entry)| entry)
        })
    }
}

fn family(key: &str, job: &str) -> bool {
    key.strip_suffix('*')
        .is_some_and(|prefix| job.starts_with(prefix))
}

/// What one revision declares for one profile's declared jobs.
#[derive(Debug, Default)]
pub struct Declared {
    /// Each declared job off the fail-safe path, with its input identity.
    identities: BTreeMap<String, Value>,
    /// Why every declared job is on the fail-safe path, when the file is missing or invalid.
    pub invalid: Option<String>,
    /// Keys that name no job of the profile; ignored.
    pub unmatched: Vec<String>,
    /// The base commit each job with `base_paths` was keyed at.
    bases: BTreeMap<String, String>,
}

impl Declared {
    /// The job's declared input identity, or None on the fail-safe path.
    pub fn identity(&self, job: &str) -> Option<&Value> {
        self.identities.get(job)
    }

    /// The base commit the job's `base_paths` were keyed at, if it has any.
    pub fn base(&self, job: &str) -> Option<&str> {
        self.bases.get(job).map(String::as_str)
    }
}

pub fn parse(text: &str) -> Result<Declarations> {
    let value: Value = serde_json::from_str(text).context("declarations are not JSON")?;
    ensure!(
        value.get("schema") == Some(&json!(SCHEMA)),
        "schema is not {SCHEMA}"
    );
    let file: File = serde_json::from_value(value)?;
    for pattern in &file.common {
        Glob::parse(pattern)?;
    }
    let mut entries = BTreeMap::new();
    for (key, raw) in file.jobs {
        let entry: Entry = serde_json::from_value(Value::Object(raw.clone()))
            .with_context(|| format!("job {key}"))?;
        match (&entry.paths, &entry.whole_tree) {
            (None, None) => bail!("job {key} has neither paths nor whole_tree"),
            (Some(_), Some(_)) => bail!("job {key} has both paths and whole_tree"),
            (Some(_), None) => ensure!(
                entry
                    .why
                    .as_ref()
                    .is_some_and(|why| why.trim().chars().count() >= 20),
                "job {key} needs a why of at least 20 characters"
            ),
            (None, Some(_)) => {}
        }
        for pattern in entry
            .paths
            .iter()
            .flatten()
            .chain(&entry.exclude)
            .chain(&entry.keep)
            .chain(&entry.base_paths)
        {
            Glob::parse(pattern).with_context(|| format!("job {key}"))?;
        }
        entries.insert(key, (entry, raw));
    }
    Ok(Declarations {
        common: file.common,
        entries,
    })
}

/// Pattern matches over one tree, each computed once.
struct Matcher<'a> {
    tree: &'a [Tracked],
    memo: BTreeMap<String, BTreeSet<usize>>,
    index: BTreeMap<&'a str, usize>,
}

impl<'a> Matcher<'a> {
    fn new(tree: &'a [Tracked]) -> Self {
        Self {
            tree,
            memo: BTreeMap::new(),
            index: tree
                .iter()
                .enumerate()
                .map(|(i, file)| (file.path.as_str(), i))
                .collect(),
        }
    }

    /// The set plus every tracked `.gitattributes` on a matched file's ancestor path: checkout
    /// applies their `eol`, `working-tree-encoding` and filters, so they change the bytes a job
    /// reads (pw#11 gate P2-2).
    fn with_attributes(&self, matched: &BTreeSet<usize>) -> BTreeSet<usize> {
        let mut all = matched.clone();
        for &i in matched {
            let path = &self.tree[i].path;
            let mut directory = path.as_str();
            loop {
                directory = directory.rsplit_once('/').map_or("", |(parent, _)| parent);
                let attributes = match directory {
                    "" => ".gitattributes".to_owned(),
                    _ => format!("{directory}/.gitattributes"),
                };
                if let Some(&j) = self.index.get(attributes.as_str()) {
                    all.insert(j);
                }
                if directory.is_empty() {
                    break;
                }
            }
        }
        all
    }

    fn matched(&mut self, pattern: &str) -> Result<&BTreeSet<usize>> {
        if !self.memo.contains_key(pattern) {
            let glob = Glob::parse(pattern)?;
            let matched = (0..self.tree.len())
                .filter(|&i| glob.matches(&self.tree[i].path))
                .collect();
            self.memo.insert(pattern.to_owned(), matched);
        }
        Ok(&self.memo[pattern])
    }

    fn union(&mut self, patterns: &[String]) -> Result<BTreeSet<usize>> {
        let mut union = BTreeSet::new();
        for pattern in patterns {
            union.extend(self.matched(pattern)?);
        }
        Ok(union)
    }

    fn nonempty(&mut self, pattern: &str, at: &str) -> Result<()> {
        ensure!(
            !self.matched(pattern)?.is_empty(),
            "pattern matches no tracked file at the {at}: {pattern}"
        );
        Ok(())
    }

    /// `[[path, mode, oid], …]` sorted by path, or None when a link is among them.
    fn files(&self, matched: &BTreeSet<usize>) -> Option<Value> {
        let mut files: Vec<&Tracked> = matched.iter().map(|&i| &self.tree[i]).collect();
        if files.iter().any(|file| file.link()) {
            return None;
        }
        files.sort_by(|a, b| a.path.cmp(&b.path));
        Some(json!(
            files
                .iter()
                .map(|file| [&file.path, &file.mode, &file.oid])
                .collect::<Vec<_>>()
        ))
    }
}

/// Validates the declarations against `tree` (and `base_paths` against `base`, when there is a
/// base) and computes each declared job's input identity. An error means the file is invalid.
pub fn evaluate(
    declarations: &Declarations,
    file: &str,
    tree: &[Tracked],
    base: Option<(&str, &[Tracked])>,
    profile: &Profile,
) -> Result<Declared> {
    let mut revision = Matcher::new(tree);
    let mut at_base = base.map(|(_, tree)| Matcher::new(tree));
    for pattern in &declarations.common {
        revision.nonempty(pattern, "revision")?;
    }
    for (entry, _) in declarations.entries.values() {
        for pattern in entry.paths.iter().flatten().chain(&entry.keep) {
            revision.nonempty(pattern, "revision")?;
        }
        if let Some(at_base) = &mut at_base {
            for pattern in &entry.base_paths {
                at_base.nonempty(pattern, "base")?;
            }
        }
    }
    // Each job keys on `common` and its own entry (schema rule 7), so the file's other bytes are
    // not an input: rewording a `why` or another job's entry reruns nothing here (pw#11 gate P3-4).
    ensure!(
        tree.iter().any(|tracked| tracked.path == file),
        "declarations file is not tracked"
    );
    let unmatched = declarations
        .entries
        .keys()
        .filter(|key| {
            !profile
                .jobs
                .iter()
                .any(|job| **key == job.name || family(key, &job.name))
        })
        .cloned()
        .collect();
    let mut identities = BTreeMap::new();
    let mut bases = BTreeMap::new();
    for job in profile
        .jobs
        .iter()
        .filter(|job| job.reuse == Reuse::Declared)
    {
        let Some((entry, raw)) = declarations.entry(&job.name) else {
            continue;
        };
        let Some(paths) = &entry.paths else {
            continue;
        };
        // `exclude` only narrows `paths`: never `common` or the declarations file.
        let excluded = revision.union(&entry.exclude)?;
        let mut inputs = revision.union(&declarations.common)?;
        inputs.extend(revision.union(paths)?.difference(&excluded));
        inputs.extend(revision.union(&entry.keep)?);
        let Some(files) = revision.files(&revision.with_attributes(&inputs)) else {
            continue;
        };
        let base_files = if entry.base_paths.is_empty() {
            json!([])
        } else {
            let Some(at_base) = &mut at_base else {
                continue;
            };
            let matched = at_base.union(&entry.base_paths)?;
            let Some(base_files) = at_base.files(&at_base.with_attributes(&matched)) else {
                continue;
            };
            if let Some((commit, _)) = base {
                bases.insert(job.name.clone(), commit.to_owned());
            }
            base_files
        };
        let mut job_entry = raw.clone();
        job_entry.remove("why");
        identities.insert(
            job.name.clone(),
            json!({"declared":{"schema":SCHEMA,
                "entry":{"common":declarations.common,"job":job_entry},
                "files":files,"base_files":base_files}}),
        );
    }
    Ok(Declared {
        identities,
        invalid: None,
        unmatched,
        bases,
    })
}

/// Reads the profile's declarations from `commit` in `repo`. Never fails: a missing or invalid
/// file, or one that cannot be read, puts every declared job on the fail-safe path.
pub fn resolve(repo: &Path, commit: &str, profile: &Profile) -> Declared {
    read(repo, commit, profile).unwrap_or_else(|error| Declared {
        invalid: Some(format!("{error:#}")),
        ..Default::default()
    })
}

fn read(repo: &Path, commit: &str, profile: &Profile) -> Result<Declared> {
    let file = profile
        .inputs_file
        .as_deref()
        .context("the profile names no inputs file")?;
    let tree = list(repo, commit)?;
    let own = tree
        .iter()
        .find(|tracked| tracked.path == file)
        .with_context(|| format!("declarations file is missing: {file}"))?;
    ensure!(
        ["100644", "100755"].contains(&own.mode.as_str()),
        "declarations file is not a regular file: {file}"
    );
    let declarations = parse(&source::git(repo, &["cat-file", "blob", &own.oid])?)?;
    let needs_base = declarations
        .entries
        .values()
        .any(|(entry, _)| !entry.base_paths.is_empty());
    let base = match needs_base {
        true => base(repo, commit)?
            .map(|base| list(repo, &base).map(|tree| (base, tree)))
            .transpose()?,
        false => None,
    };
    let base = base
        .as_ref()
        .map(|(commit, tree)| (commit.as_str(), tree.as_slice()));
    evaluate(&declarations, file, &tree, base, profile)
}

/// `git ls-tree -r -z --full-tree`: every tracked file of `commit`.
fn list(repo: &Path, commit: &str) -> Result<Vec<Tracked>> {
    let output = source::git(repo, &["ls-tree", "-r", "-z", "--full-tree", commit])?;
    output
        .split('\0')
        .filter(|line| !line.is_empty())
        .map(|line| {
            let (metadata, path) = line.split_once('\t').context("invalid git entry")?;
            let fields: Vec<_> = metadata.split(' ').collect();
            ensure!(fields.len() == 3, "invalid git entry");
            Ok(Tracked {
                path: path.into(),
                mode: fields[0].into(),
                oid: fields[2].into(),
            })
        })
        .collect()
}

/// The commit a `Patchweft-Base:` trailer names. No trailer, or one that is ambiguous or names no
/// commit here, leaves no base.
fn base(repo: &Path, commit: &str) -> Result<Option<String>> {
    let trailers = source::git(
        repo,
        &[
            "show",
            "-s",
            "--format=%(trailers:key=Patchweft-Base,valueonly)",
            commit,
        ],
    )?;
    let named: BTreeSet<&str> = trailers
        .lines()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .collect();
    if !named.is_empty() {
        let [value] = named.into_iter().collect::<Vec<_>>()[..] else {
            return Ok(None);
        };
        if !((value.len() == 40 || value.len() == 64)
            && value.bytes().all(|c| c.is_ascii_hexdigit()))
        {
            return Ok(None);
        }
        let output = source::git_command(repo)
            .args([
                "rev-parse",
                "--verify",
                "--quiet",
                &format!("{value}^{{commit}}"),
            ])
            .output()?;
        return Ok(output
            .status
            .success()
            .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned()));
    }
    // No trailer, no base: a composition's first parent is an intermediate step, not its base
    // (pw#11 gate P2-3), so a job with `base_paths` keeps the commit key.
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source;
    use serde_json::json;
    use std::{fs, path::PathBuf};

    const WHY: &str = "measured reads of the fixture job";
    const FILE: &str = "ci/inputs.json";

    fn glob(pattern: &str) -> Glob {
        Glob::parse(pattern).unwrap()
    }

    #[test]
    fn star_stays_in_a_segment_and_matches_hidden_names() {
        assert!(glob("*.md").matches("README.md"));
        assert!(!glob("*.md").matches("docs/README.md"));
        assert!(!glob("docs/*").matches("docs/a/b.md"));
        assert!(glob("docs/*").matches("docs/.hidden"));
        assert!(glob("*").matches(".github"));
        assert!(glob(".*").matches(".env"));
        assert!(glob("a?c").matches("abc"));
        assert!(!glob("a?c").matches("a/c"));
        assert!(!glob("a?c").matches("ac"));
        assert!(glob("ä?").matches("äö"));
        assert!(!glob("*.MD").matches("README.md"), "case-sensitive");
    }

    #[test]
    fn double_star_spans_whole_segments_and_patterns_are_anchored() {
        for path in ["a/b", "a/x/b", "a/x/y/b"] {
            assert!(glob("a/**/b").matches(path), "{path}");
        }
        assert!(!glob("a/**/b").matches("a/b/c"));
        assert!(!glob("a/**/b").matches("xa/b"));
        for path in ["x", ".hidden", "a/b/c", "a/.b/c"] {
            assert!(glob("**").matches(path), "{path}");
        }
        assert!(glob("**/package.json").matches("package.json"));
        assert!(glob("**/package.json").matches("packages/db/package.json"));
        assert!(glob("docs/**").matches("docs/a/b.md"));
        assert!(!glob("b").matches("a/b"), "anchored at the root");
        assert!(!glob("a").matches("a/b"), "a whole path, not a prefix");
        assert!(glob("a/b").matches("a/b"));
    }

    #[test]
    fn patterns_outside_the_dialect_are_refused() {
        for pattern in [
            "a{b,c}", "a}", "[ab]", "a]", "!a", "a\\b", "a//b", "a/", "", "/a", "a**", "**b",
            "a/***/b",
        ] {
            assert!(Glob::parse(pattern).is_err(), "{pattern:?}");
        }
        for pattern in ["a!b", "a/**", "**/b", "a b/*.c", "?", "a-*"] {
            assert!(Glob::parse(pattern).is_ok(), "{pattern:?}");
        }
    }

    fn tracked(path: &str, mode: &str) -> Tracked {
        Tracked {
            path: path.into(),
            mode: mode.into(),
            oid: crate::hash(format!("{path}{mode}").as_bytes()),
        }
    }

    fn tree(paths: &[&str]) -> Vec<Tracked> {
        paths.iter().map(|p| tracked(p, "100644")).collect()
    }

    const PATHS: &[&str] = &[
        FILE,
        "package.json",
        "pnpm-lock.yaml",
        "src/a.ts",
        "src/b.ts",
        "src/README.md",
        "docs/guide.md",
        "docs/keep.md",
        "scripts/run.sh",
        "migrations/1.sql",
    ];

    fn profile(jobs: &[(&str, &str)]) -> Profile {
        let jobs: Vec<Value> = jobs
            .iter()
            .map(|(name, reuse)| {
                let mut job = json!({"name":name,"command":["sh"],"reuse":reuse});
                if *reuse == "declared" {
                    job["audit"] = json!(WHY);
                }
                job
            })
            .collect();
        serde_json::from_value(json!({"version":1,"name":"p","tools":{},"inputs_file":FILE,
            "jobs":jobs}))
        .unwrap()
    }

    fn declared(jobs: &[&str]) -> Profile {
        profile(&jobs.iter().map(|j| (*j, "declared")).collect::<Vec<_>>())
    }

    fn evaluated(
        text: &Value,
        tree: &[Tracked],
        base: Option<&[Tracked]>,
        p: &Profile,
    ) -> Result<Declared> {
        let base = base.map(|tree| ("b".repeat(40), tree));
        let base = base.as_ref().map(|(commit, tree)| (commit.as_str(), *tree));
        evaluate(&parse(&text.to_string())?, FILE, tree, base, p)
    }

    fn invalid(text: Value, expected: &str) {
        let tree = tree(PATHS);
        let base = tree.clone();
        let error = evaluated(&text, &tree, Some(&base), &declared(&["lint"]))
            .map(|d| format!("valid: {d:?}"))
            .unwrap_or_else(|e| format!("{e:#}"));
        assert!(
            error.contains(expected),
            "{expected:?} not in {error:?} for {text}"
        );
    }

    fn file(jobs: Value) -> Value {
        json!({"schema":SCHEMA,"common":["package.json"],"jobs":jobs})
    }

    fn files(declared: &Declared, job: &str) -> Vec<String> {
        declared.identity(job).unwrap()["declared"]["files"]
            .as_array()
            .unwrap()
            .iter()
            .map(|f| f[0].as_str().unwrap().to_owned())
            .collect()
    }

    #[test]
    fn unknown_fields_another_schema_and_bad_entries_are_invalid() {
        let lint = |entry: Value| file(json!({"lint":entry}));
        invalid(
            json!({"schema":SCHEMA,"jobs":{},"extra":1}),
            "unknown field `extra`",
        );
        invalid(json!({"schema":"patchweft.inputs/2","jobs":{}}), "schema");
        invalid(json!({"jobs":{}}), "schema");
        invalid(
            lint(json!({"paths":["src/**"],"why":WHY,"reads":[]})),
            "unknown field `reads`",
        );
        invalid(lint(json!({"why":WHY})), "neither");
        invalid(
            lint(json!({"paths":["src/**"],"whole_tree":"x","why":WHY})),
            "both",
        );
        invalid(lint(json!({"paths":["src/**"]})), "why");
        invalid(lint(json!({"paths":["src/**"],"why":"too short"})), "why");
        invalid(lint(json!({"paths":"src/**","why":WHY})), "invalid type");
    }

    #[test]
    fn every_pattern_list_is_held_to_the_dialect() {
        for bad in [
            "src/{a,b}.ts",
            "[s]rc/**",
            "!src/**",
            "src\\a.ts",
            "src//a.ts",
            "/src/**",
            "src/a**",
        ] {
            invalid(json!({"schema":SCHEMA,"common":[bad],"jobs":{}}), bad);
            for field in ["paths", "exclude", "keep", "base_paths"] {
                let mut entry = json!({"paths":["src/**"],"why":WHY});
                entry[field] = json!([bad]);
                invalid(file(json!({"lint":entry})), bad);
            }
        }
    }

    #[test]
    fn a_pattern_matching_no_tracked_file_is_invalid() {
        invalid(
            json!({"schema":SCHEMA,"common":["nothing/**"],"jobs":{}}),
            "nothing/**",
        );
        for field in ["paths", "keep", "base_paths"] {
            let mut entry = json!({"paths":["src/**"],"why":WHY});
            entry[field] = json!(["nothing/**"]);
            invalid(file(json!({"lint":entry})), "nothing/**");
        }
        // `base_paths` is matched against the base, not the revision.
        let revision = tree(PATHS);
        let base = tree(&[FILE, "package.json"]);
        let text =
            file(json!({"lint":{"paths":["src/**"],"base_paths":["migrations/**"],"why":WHY}}));
        let error = evaluated(&text, &revision, Some(&base), &declared(&["lint"])).unwrap_err();
        assert!(format!("{error:#}").contains("migrations/**"), "{error:#}");
        // An unmatched exclusion is only a no-op.
        let text = file(json!({"lint":{"paths":["src/**"],"exclude":["nothing/**"],"why":WHY}}));
        assert!(evaluated(&text, &revision, None, &declared(&["lint"])).is_ok());
    }

    #[test]
    fn exact_names_beat_families_and_unmatched_keys_are_ignored() {
        let text = file(json!({
            "vitest-*":{"paths":["src/**"],"why":WHY},
            "vitest-unit-*":{"paths":["scripts/**"],"why":WHY},
            "vitest-exact":{"paths":["docs/**"],"why":WHY},
            "ghost":{"paths":["src/**"],"why":WHY},
            "ghost-*":{"paths":["src/**"],"why":WHY},
        }));
        let p = declared(&["vitest-a", "vitest-unit-b", "vitest-exact", "other"]);
        let tree = tree(PATHS);
        let result = evaluated(&text, &tree, None, &p).unwrap();
        assert_eq!(result.invalid, None);
        assert_eq!(result.unmatched, ["ghost", "ghost-*"]);
        assert_eq!(
            files(&result, "vitest-a"),
            ["package.json", "src/README.md", "src/a.ts", "src/b.ts"]
        );
        assert_eq!(
            files(&result, "vitest-unit-b"),
            ["package.json", "scripts/run.sh"]
        );
        assert_eq!(
            files(&result, "vitest-exact"),
            ["docs/guide.md", "docs/keep.md", "package.json"]
        );
        assert_eq!(result.identity("other"), None, "absent from jobs");
        // A key naming only a job of another policy still names a job of the profile.
        let mixed = profile(&[("ghost", "inflight"), ("ghost-1", "inflight")]);
        let mixed = evaluated(&text, &tree, None, &mixed).unwrap();
        assert_eq!(
            mixed.unmatched,
            ["vitest-*", "vitest-exact", "vitest-unit-*"]
        );
        assert_eq!(mixed.identity("ghost"), None, "not a declared job");
    }

    #[test]
    fn whole_tree_absent_base_and_links_take_the_fail_safe_path() {
        let text = file(json!({
            "census":{"whole_tree":"lists the whole tree"},
            "proofs":{"paths":["scripts/**"],"base_paths":["migrations/**"],"why":WHY},
            "linked":{"paths":["src/**"],"why":WHY},
            "module":{"paths":["src/**"],"why":WHY},
        }));
        let p = declared(&["census", "proofs", "linked", "module"]);
        let mut revision = tree(PATHS);
        revision.push(tracked("src/link", "120000"));
        let result = evaluated(&text, &revision, None, &p).unwrap();
        assert_eq!(result.invalid, None);
        for job in ["census", "proofs", "linked", "module"] {
            assert_eq!(result.identity(job), None, "{job}");
        }
        let mut gitlink = tree(PATHS);
        gitlink.push(tracked("src/vendor", "160000"));
        let base = tree(PATHS);
        let result = evaluated(&text, &gitlink, Some(&base), &p).unwrap();
        assert_eq!(result.identity("module"), None, "gitlink");
        let proofs = result.identity("proofs").unwrap();
        assert_eq!(
            proofs["declared"]["base_files"][0][0],
            json!("migrations/1.sql")
        );
        // A link at the base is in the matched set too.
        let mut linked_base = tree(PATHS);
        linked_base.push(tracked("migrations/2.sql", "120000"));
        let result = evaluated(&text, &tree(PATHS), Some(&linked_base), &p).unwrap();
        assert_eq!(result.identity("proofs"), None);
    }

    #[test]
    fn inputs_are_common_plus_paths_minus_exclude_plus_keep() {
        let text = json!({"schema":SCHEMA,"common":["package.json","docs/guide.md"],"jobs":{
            "lint":{"paths":["**"],"exclude":["docs/**","**/*.md",FILE,"package.json"],
                    "keep":["docs/keep.md"],"why":WHY}}});
        let tree = tree(PATHS);
        let result = evaluated(&text, &tree, None, &declared(&["lint"])).unwrap();
        assert_eq!(
            files(&result, "lint"),
            [
                "docs/guide.md",
                "docs/keep.md",
                "migrations/1.sql",
                "package.json",
                "pnpm-lock.yaml",
                "scripts/run.sh",
                "src/a.ts",
                "src/b.ts"
            ]
        );
        let identity = result.identity("lint").unwrap();
        let entry = &identity["declared"]["entry"];
        assert_eq!(identity["declared"]["schema"], SCHEMA);
        assert_eq!(entry["common"], json!(["package.json", "docs/guide.md"]));
        assert!(entry["job"].get("why").is_none());
        assert_eq!(entry["job"]["keep"], json!(["docs/keep.md"]));
        assert_eq!(identity["declared"]["base_files"], json!([]));
        let guide = tree
            .iter()
            .find(|file| file.path == "docs/guide.md")
            .unwrap();
        let first = &identity["declared"]["files"][0];
        assert_eq!(first, &json!(["docs/guide.md", "100644", guide.oid]));
        // The reason is not identity; the entry is.
        let reworded = text
            .to_string()
            .replace(WHY, "a different explanation of reads");
        let reworded = evaluate(
            &parse(&reworded).unwrap(),
            FILE,
            &tree,
            None,
            &declared(&["lint"]),
        )
        .unwrap();
        assert_eq!(reworded.identity("lint"), Some(identity));
        // Key order as written is not identity: the entry is hashed canonically.
        let reordered = format!(
            r#"{{"jobs":{{"lint":{{"why":"{WHY}","keep":["docs/keep.md"],"paths":["**"],
                "exclude":["docs/**","**/*.md","{FILE}","package.json"]}}}},
                "common":["package.json","docs/guide.md"],"schema":"{SCHEMA}"}}"#
        );
        let reordered = evaluate(
            &parse(&reordered).unwrap(),
            FILE,
            &tree,
            None,
            &declared(&["lint"]),
        )
        .unwrap();
        assert_eq!(reordered.identity("lint"), Some(identity));
    }

    /// A file written (Some) or deleted (None).
    type Change<'a> = (&'a str, Option<&'a str>);

    /// A working repository whose commits stand in for revisions.
    struct Repo {
        _temp: tempfile::TempDir,
        path: PathBuf,
    }

    impl Repo {
        fn new() -> Self {
            let temp = tempfile::tempdir().unwrap();
            let path = temp.path().join("repo");
            fs::create_dir(&path).unwrap();
            source::git(&path, &["init", "--quiet"]).unwrap();
            Self { _temp: temp, path }
        }
        /// Writes (Some) or deletes (None) each file, then commits; returns the commit.
        fn commit(&self, files: &[Change], message: &str) -> String {
            for (name, text) in files {
                let target = self.path.join(name);
                match text {
                    Some(text) => {
                        fs::create_dir_all(target.parent().unwrap()).unwrap();
                        fs::write(target, text).unwrap();
                    }
                    None => fs::remove_file(target).unwrap(),
                }
            }
            self.git(&["add", "-A"]);
            self.git(&["commit", "--quiet", "--allow-empty", "-m", message]);
            self.git(&["rev-parse", "HEAD"])
        }
        fn git(&self, args: &[&str]) -> String {
            let mut all = vec![
                "-c",
                "user.name=Fixture",
                "-c",
                "user.email=fixture@local.invalid",
            ];
            all.extend(args);
            source::git(&self.path, &all).unwrap()
        }
    }

    const DECLARATIONS: &str = r#"{"schema":"patchweft.inputs/1","common":["package.json"],
        "jobs":{"lint":{"paths":["src/**"],"exclude":["**/*.md"],"why":"lint reads src and package.json"},
                "proofs":{"paths":["scripts/**"],"base_paths":["migrations/**"],
                          "why":"proofs compare migrations with the base"}}}"#;

    fn seeded() -> (Repo, String) {
        let repo = Repo::new();
        let commit = repo.commit(
            &[
                (FILE, Some(DECLARATIONS)),
                ("package.json", Some("{}")),
                ("src/a.ts", Some("a")),
                ("src/b.ts", Some("b")),
                ("src/notes.md", Some("notes")),
                ("scripts/run.sh", Some("run")),
                ("migrations/1.sql", Some("one")),
                ("README", Some("readme")),
            ],
            "seed",
        );
        (repo, commit)
    }

    fn lint(repo: &Repo, commit: &str) -> Option<Value> {
        let result = resolve(&repo.path, commit, &declared(&["lint", "proofs"]));
        assert_eq!(result.invalid, None);
        result.identity("lint").cloned()
    }

    #[test]
    fn identity_follows_declared_files_only() {
        let (repo, seed) = seeded();
        let first = lint(&repo, &seed).unwrap();
        let outside = repo.commit(
            &[
                ("README", Some("changed")),
                ("src/notes.md", Some("excluded")),
            ],
            "outside",
        );
        assert_eq!(lint(&repo, &outside).unwrap(), first);
        let edits: [(&str, &[Change]); 4] = [
            ("content", &[("src/a.ts", Some("a2"))]),
            ("rename", &[("src/a.ts", None), ("src/c.ts", Some("a"))]),
            ("addition", &[("src/d.ts", Some("d"))]),
            ("deletion", &[("src/b.ts", None)]),
        ];
        for (kind, edit) in edits {
            repo.git(&["reset", "--quiet", "--hard", &outside]);
            let changed = repo.commit(edit, kind);
            assert_ne!(lint(&repo, &changed).unwrap(), first, "{kind}");
        }
        repo.git(&["reset", "--quiet", "--hard", &outside]);
        let common = repo.commit(&[("package.json", Some("{\"a\":1}"))], "common");
        assert_ne!(lint(&repo, &common).unwrap(), first, "common");
        repo.git(&["reset", "--quiet", "--hard", &outside]);
        let reworded = repo.commit(
            &[(
                FILE,
                Some(&DECLARATIONS.replace("lint reads", "lint only reads")),
            )],
            "reword",
        );
        assert_eq!(
            lint(&repo, &reworded).unwrap(),
            first,
            "a reworded why is not an input (schema rule 7)"
        );
    }

    #[test]
    fn base_files_come_only_from_the_trailer() {
        let (repo, seed) = seeded();
        let p = declared(&["lint", "proofs"]);
        let resolved = |commit: &str| resolve(&repo.path, commit, &p);
        let proofs = |commit: &str| resolved(commit).identity("proofs").cloned();
        // No trailer, no base: a composition's first parent is not its base (pw#11 gate P2-3).
        assert_eq!(proofs(&seed), None);
        let child = repo.commit(&[("README", Some("child"))], "child");
        assert_eq!(proofs(&child), None, "the parent is never guessed");
        assert!(resolved(&child).identity("lint").is_some());
        let named = repo.commit(
            &[("README", Some("named"))],
            &format!("named base\n\nPatchweft-Base: {seed}"),
        );
        let seeded_base = proofs(&named).unwrap();
        assert_eq!(
            seeded_base["declared"]["base_files"],
            json!([[
                "migrations/1.sql",
                "100644",
                repo.git(&["rev-parse", &format!("{seed}:migrations/1.sql")])
            ]])
        );
        assert_eq!(
            resolved(&named).base("proofs"),
            Some(seed.as_str()),
            "the keyed base is recorded"
        );
        assert_eq!(
            resolved(&named).base("lint"),
            None,
            "a job without base_paths keys no base"
        );
        let migrated = repo.commit(
            &[("migrations/1.sql", Some("one, edited"))],
            &format!("migrate\n\nPatchweft-Base: {seed}"),
        );
        assert_eq!(
            proofs(&migrated).unwrap(),
            seeded_base,
            "its own migrations are not base files"
        );
        let next = repo.commit(
            &[("README", Some("next"))],
            &format!("next\n\nPatchweft-Base: {migrated}"),
        );
        assert_ne!(
            proofs(&next).unwrap(),
            seeded_base,
            "the base's migrations changed"
        );
        // A trailer that names no commit leaves no base rather than falling back.
        let unknown = repo.commit(
            &[("README", Some("unknown"))],
            &format!("unknown base\n\nPatchweft-Base: {}", "0".repeat(40)),
        );
        assert_eq!(proofs(&unknown), None);
        assert!(resolved(&unknown).identity("lint").is_some());
    }

    #[test]
    fn a_tracked_symlink_takes_the_fail_safe_path() {
        let (repo, _) = seeded();
        std::os::unix::fs::symlink("a.ts", repo.path.join("src/link.ts")).unwrap();
        let linked = repo.commit(&[], "link");
        assert_eq!(lint(&repo, &linked), None);
    }

    #[test]
    fn a_missing_or_unreadable_file_is_invalid() {
        let repo = Repo::new();
        let bare = repo.commit(&[("package.json", Some("{}"))], "bare");
        let result = resolve(&repo.path, &bare, &declared(&["lint"]));
        assert!(result.invalid.as_deref().unwrap().contains("missing"));
        let broken = repo.commit(&[(FILE, Some("{not json"))], "broken");
        let result = resolve(&repo.path, &broken, &declared(&["lint"]));
        assert!(result.invalid.is_some());
        assert_eq!(result.identity("lint"), None);
    }
}
