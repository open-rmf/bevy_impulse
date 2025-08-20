/**
 * The root namespace is an empty string so namespaces that begins with `:` can be identified as
 * absolute.
 */
export const ROOT_NAMESPACE = '';

export function joinNamespaces(...namespaces: string[]): string {
  return namespaces.join(':');
}

export function splitNamespaces(namespace: string): string[] {
  return namespace.split(':');
}
