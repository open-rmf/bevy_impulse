export function joinNamespaces(...namespaces: string[]): string {
  return namespaces.join(':');
}

export function splitNamespaces(namespace: string): string[] {
  return namespace.split(':');
}
