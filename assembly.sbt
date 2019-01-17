assemblyMergeStrategy in assembly := {
    case n if n.startsWith("META-INF") => MergeStrategy.discard
    case n if n.contains("META-INF/MANIFEST.MF") => MergeStrategy.discard
    case n if n.contains("commons-beanutils") => MergeStrategy.discard
    case n if n.contains("Log$Logger.class") => MergeStrategy.last
    case n if n.contains("Log.class") => MergeStrategy.last
    case _ => MergeStrategy.first
}

test in assembly := {}
